package hvfhv.speed

import hvfhv.common.CassandraSchema.Speed
import hvfhv.common._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory

/** Where the speed layer upserts per-micro-batch totals. */
trait SpeedSink {
  def upsert(rows: DataFrame): Unit
}

final class CassandraSpeedSink(keyspace: String) extends SpeedSink {
  override def upsert(rows: DataFrame): Unit = CassandraIO.write(rows, keyspace, CassandraSchema.SpeedTable)
}

/**
 * Applies one micro-batch of trips to the speed view.
 *
 * Each micro-batch's totals are written as their own rows, keyed by route, request date,
 * streaming query id and micro-batch id. Spark replays a micro-batch with exactly the same
 * Kafka offsets (hence the same trips and the same totals) when a query restarts before
 * the batch was marked complete, so a replay rewrites identical rows: replays and partial
 * writes can never double-count. Readers sum a route's rows.
 *
 * @param zones taxi zone LocationID -> zone name, the same lookup the batch layer uses
 */
final class SpeedWriter(zones: Map[Int, String], sink: SpeedSink) {

  private val log = LoggerFactory.getLogger(getClass)

  /** Kept as a function value: in Scala 2.12 a lambda passed to foreachBatch is ambiguous. */
  val asForeachBatch: (DataFrame, Long) => Unit = process

  def process(trips: DataFrame, batchId: Long): Unit = {
    // Stable across restarts from the same checkpoint, so replays land on the same rows.
    val queryId = Option(trips.sparkSession.sparkContext.getLocalProperty(SpeedWriter.QueryIdProperty)).getOrElse("default")
    val rows = TripTransforms.aggregate(TripTransforms.enrich(trips, zones), byRequestDate = true)
      .withColumn(Speed.QueryId, lit(queryId))
      .withColumn(Speed.BatchId, lit(batchId))
    sink.upsert(rows)
    log.info(s"Applied micro-batch $batchId of query $queryId to the speed view")
  }
}

object SpeedWriter {
  /** Set by Spark on the thread that runs a streaming query's micro-batches. */
  val QueryIdProperty: String = "sql.streaming.queryId"
}
