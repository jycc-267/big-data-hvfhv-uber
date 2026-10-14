package hvfhv.batch

import java.time.Instant

import com.datastax.spark.connector.cql.CassandraConnector
import hvfhv.common.CassandraSchema.Speed
import hvfhv.common._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Brings the master dataset up to date and publishes a new batch view atomically:
 *
 *  1. absorb the speed layer's landing zone into Hive (`trips_stream`);
 *  2. aggregate every trip with request date < watermark into a brand-new Cassandra table;
 *  3. verify the table holds every aggregated row;
 *  4. flip the `serving` pointer (table + watermark) with a single-partition write;
 *  5. retire what no reader can still be using: batch tables older than the previous one
 *     and speed rows dated before both the previous and the new watermark.
 *
 * Readers therefore see either the old view with the old watermark or the new view with the
 * new watermark, never a half-written table or a mismatched pair. Step 5 lags by one run so
 * a reader holding a cached pointer to the previous view still gets complete answers.
 *
 * Usage: BuildBatchView [--watermark yyyyMMdd]
 */
object BuildBatchView {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val options = CliArgs.parse(args, Set("watermark"))
    val config = AppConfig.load()
    val spark = SparkSessions.forJob("hvfhv-build-batch-view", config)
    try run(spark, config, options.get("watermark").map(Watermark.validate))
    finally spark.stop()
  }

  def run(spark: SparkSession, config: AppConfig, requestedWatermark: Option[String]): ServingPointer = {
    val tables = config.hive.tables
    MasterDataset.absorbStream(spark, config.paths.landingEvents, config.paths.staging, tables)

    val master = MasterDataset.read(spark, tables)
    val watermark = requestedWatermark
      .orElse(Watermark.choose(MasterDataset.maxRequestDate(master.bulk), MasterDataset.maxRequestDate(master.stream)))
      .getOrElse(throw new IllegalStateException("The master dataset is empty; run IngestMasterDataset first"))
    val zones = Zones.toMap(Zones.readMaster(spark, tables))

    val view = compute(master.trips, zones, watermark).cache()
    try {
      val rows = view.count()
      log.info(s"Batch view covers request dates < $watermark: $rows routes")
      publish(spark, config.cassandra.keyspace, view, rows, watermark)
    } finally view.unpersist()
  }

  /** The batch view: one aggregate row per route over every trip requested before `watermark`. */
  def compute(trips: DataFrame, zones: Map[Int, String], watermark: String): DataFrame =
    TripTransforms.aggregate(
      TripTransforms.enrich(trips.where(col(TripSchema.RequestDate) < watermark), zones),
      byRequestDate = false)

  private def publish(spark: SparkSession, keyspace: String, view: DataFrame, rows: Long, watermark: String): ServingPointer = {
    val connector = CassandraIO.connector(spark)
    val newTable = CassandraSchema.batchView(System.currentTimeMillis())
    connector.withSessionDo { session =>
      CassandraIO.ensureServingSchema(session, keyspace)
      session.execute(CassandraSchema.createBatchView(keyspace, newTable))
    }

    CassandraIO.write(view, keyspace, newTable)
    val written = CassandraIO.read(spark, keyspace, newTable).count()
    if (written != rows) {
      connector.withSessionDo(CassandraIO.dropTable(_, keyspace, newTable))
      throw new IllegalStateException(s"$newTable has $written rows, expected $rows; serving pointer left unchanged")
    }

    val pointer = ServingPointer(newTable, watermark, Instant.now().toString)
    val previous = connector.withSessionDo { session =>
      val previous = ServingPointer.read(session, keyspace)
      ServingPointer.write(session, keyspace, pointer)
      log.info(s"Serving $newTable (request dates < $watermark); previous: ${previous.getOrElse("none")}")

      val keep = Set(newTable) ++ previous.map(_.batchTable)
      CassandraIO.tables(session, keyspace)
        .filter(t => CassandraSchema.isBatchView(t) && !keep(t))
        .foreach { t =>
          CassandraIO.dropTable(session, keyspace, t)
          log.info(s"Dropped retired batch view $t")
        }
      previous
    }

    // min() keeps a deliberate rollback to an older --watermark from losing speed rows it needs.
    previous.map(p => Seq(p.watermark, watermark).min).foreach { cutoff =>
      val deleted = deleteSpeedRowsBefore(spark, connector, keyspace, cutoff)
      log.info(s"Deleted $deleted speed rows dated before $cutoff, covered by both live batch views")
    }
    pointer
  }

  /** One range delete per route partition that has rows older than `cutoff`. */
  private def deleteSpeedRowsBefore(spark: SparkSession, connector: CassandraConnector, keyspace: String, cutoff: String): Long = {
    val stale = CassandraIO.read(spark, keyspace, CassandraSchema.SpeedTable).where(col(Speed.RequestDate) < cutoff).cache()
    try {
      val count = stale.count()
      val statement = CassandraSchema.deleteSpeedBefore(keyspace)
      stale.select(TripTransforms.RouteColumns.map(col): _*).distinct().rdd.foreachPartition { routes =>
        connector.withSessionDo { session =>
          val delete = session.prepare(statement)
          routes.foreach { r =>
            session.execute(delete.bind(r.getString(0), r.getString(1), r.getString(2), Int.box(r.getInt(3)), cutoff))
          }
        }
      }
      count
    } finally stale.unpersist()
  }
}
