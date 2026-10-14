package hvfhv.batch

import hvfhv.common.{AppConfig, SparkSessions, TripSchema, Zones}
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

/**
 * Loads the downloaded CSVs into the Hive master tables `trips_bulk` and `zones`.
 * Re-running replaces both tables instead of appending, so the job is idempotent.
 */
object IngestMasterDataset {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val tables = config.hive.tables
    val spark = SparkSessions.forJob("hvfhv-ingest-master", config)
    try {
      MasterDataset.replaceBulk(spark, TripSchema.normalize(spark.read.option("header", "true").csv(config.paths.rawTrips)), tables)
      Zones.parseCsv(spark, config.paths.rawZones).write.mode(SaveMode.Overwrite).insertInto(tables.zones)

      val bulk = MasterDataset.read(spark, tables).bulk
      log.info(s"${tables.bulk}: ${bulk.count()} trips, newest request date " +
        s"${MasterDataset.maxRequestDate(bulk).getOrElse("n/a")}")
    } finally spark.stop()
  }
}
