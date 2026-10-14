package hvfhv.batch

import hvfhv.common.CassandraSchema.Speed
import hvfhv.common._
import org.apache.spark.sql.functions.{coalesce, col, lit, sum}
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.LoggerFactory

/**
 * Verifies the Lambda invariant end to end: for every route, what the web app serves
 * (active batch view + speed rows dated on or after the watermark) must equal a fresh
 * computation over the entire master dataset in Hive. Exits non-zero on any difference.
 *
 * The expected side also includes landing-zone trips that no batch run has absorbed yet,
 * since the speed view already counts them. The check only reads; it never writes to Hive.
 * Run it once the pipeline is quiescent (producer stopped, speed layer caught up), since
 * in-flight trips legitimately make the two sides differ.
 */
object ReconcileCheck {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val keyspace = config.cassandra.keyspace
    val spark = SparkSessions.forJob("hvfhv-reconcile-check", config)
    val failures = try {
      val tables = config.hive.tables
      val hive = MasterDataset.read(spark, tables)
      val master = MasterDataset.readLanding(spark, config.paths.landingEvents)
        .fold(hive)(landed => hive.copy(stream = hive.stream.unionByName(landed)))
      val zones = Zones.toMap(Zones.readMaster(spark, tables))
      val expected = TripTransforms.aggregate(TripTransforms.enrich(master.trips, zones), byRequestDate = false)

      val pointer = CassandraIO.connector(spark).withSessionDo(ServingPointer.read(_, keyspace))
        .getOrElse(throw new IllegalStateException("No serving pointer; run BuildBatchView first"))
      val served = Reconciliation.served(
        CassandraIO.read(spark, keyspace, pointer.batchTable),
        CassandraIO.read(spark, keyspace, CassandraSchema.SpeedTable),
        pointer.watermark)

      val mismatches = Reconciliation.mismatches(expected, served).cache()
      log.info(s"Checking ${pointer.batchTable} (< ${pointer.watermark}) + speed rows >= ${pointer.watermark} " +
        s"against ${expected.count()} routes recomputed from the Hive master dataset")
      val n = mismatches.count()
      if (n > 0) mismatches.show(20, truncate = false)
      n
    } finally spark.stop()

    if (failures == 0) log.info("Reconciliation passed: serving layer matches the master dataset")
    else {
      log.error(s"Reconciliation FAILED: $failures routes differ")
      sys.exit(1)
    }
  }
}

object Reconciliation {

  private val Route = TripTransforms.RouteColumns
  private val Metric = Metrics.Columns

  /** What a reader sees per route: the batch row plus every speed row dated >= watermark. */
  def served(batch: DataFrame, speed: DataFrame, watermark: String): DataFrame = {
    val shape = (Route ++ Metric).map(col)
    batch.select(shape: _*)
      .unionByName(speed.where(col(Speed.RequestDate) >= watermark).select(shape: _*))
      .groupBy(Route.map(col): _*)
      .agg(sum(col(Metric.head)).as(Metric.head), Metric.tail.map(c => sum(col(c)).as(c)): _*)
  }

  /** Routes whose served metrics differ from the expected ones, side by side. */
  def mismatches(expected: DataFrame, served: DataFrame): DataFrame = {
    def prefixed(df: DataFrame, prefix: String) =
      df.select(Route.map(col) ++ Metric.map(c => col(c).as(s"$prefix$c")): _*)
    def value(prefix: String, c: String): Column = coalesce(col(s"$prefix$c"), lit(0L))

    prefixed(expected, "expected_")
      .join(prefixed(served, "served_"), Route, "full_outer")
      .where(Metric.map(c => value("expected_", c) =!= value("served_", c)).reduce(_ || _))
  }
}
