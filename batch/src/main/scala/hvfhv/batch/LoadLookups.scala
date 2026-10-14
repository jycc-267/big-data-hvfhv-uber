package hvfhv.batch

import hvfhv.common.CassandraSchema.Lookups
import hvfhv.common._
import org.apache.spark.sql.functions.{col, lit}
import org.slf4j.LoggerFactory

/** Fills the Cassandra `lookups` table behind the web app's carrier, zone and hour drop-downs. */
object LoadLookups {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val keyspace = config.cassandra.keyspace
    val spark = SparkSessions.forJob("hvfhv-load-lookups", config)
    try {
      import spark.implicits._
      CassandraIO.connector(spark).withSessionDo(CassandraIO.ensureServingSchema(_, keyspace))

      val zones = Zones.readMaster(spark, config.hive.tables)
        .where(col("zone").isNotNull).select(lit(Lookups.Zone).as(Lookups.Kind), col("zone").as(Lookups.Value))
      val fixed = (Carriers.Names.map(Lookups.Carrier -> _) ++ (0 to 23).map(h => Lookups.Hour -> h.toString))
        .toDF(Lookups.Kind, Lookups.Value)
      val rows = fixed.unionByName(zones).distinct()

      CassandraIO.write(rows, keyspace, CassandraSchema.LookupsTable)
      log.info(s"Loaded ${rows.count()} lookup values into $keyspace.${CassandraSchema.LookupsTable}")
    } finally spark.stop()
  }
}
