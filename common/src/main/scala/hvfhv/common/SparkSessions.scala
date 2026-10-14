package hvfhv.common

import org.apache.spark.sql.SparkSession

object SparkSessions {

  /**
   * Settings every job must share, or batch and speed would bucket the same trip differently:
   *  - Trip timestamps are naive New York wall-clock times. A UTC session keeps hour() and
   *    the request date equal to the wall clock and avoids DST gaps/overlaps.
   *  - CORRECTED makes an unparseable timestamp a null instead of an upgrade exception.
   *  - Dynamic overwrite replaces only the partitions (request dates) a write touches.
   */
  val Required: Map[String, String] = Map(
    "spark.sql.session.timeZone" -> "UTC",
    "spark.sql.legacy.timeParserPolicy" -> "CORRECTED",
    "spark.sql.sources.partitionOverwriteMode" -> "dynamic")

  def builder(appName: String): SparkSession.Builder =
    Required.foldLeft(SparkSession.builder().appName(appName)) { case (b, (k, v)) => b.config(k, v) }

  /** A production job: Hive Metastore catalog for the master tables, Cassandra for serving. */
  def forJob(appName: String, config: AppConfig): SparkSession =
    builder(appName)
      .enableHiveSupport()
      .config("spark.hadoop.hive.metastore.uris", config.hive.metastoreUri)
      // Spark's built-in Hive 2.3 client calls metastore APIs that Hive 4 removed; the 3.1.3
      // client works with it. Spark downloads it once (cached in ~/.ivy2).
      .config("spark.sql.hive.metastore.version", "3.1.3")
      .config("spark.sql.hive.metastore.jars", "maven")
      .config("spark.cassandra.connection.host", config.cassandra.host)
      .config("spark.cassandra.connection.port", config.cassandra.port.toString)
      .getOrCreate()
}
