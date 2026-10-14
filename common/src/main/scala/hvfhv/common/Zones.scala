package hvfhv.common

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** NYC taxi zone lookup (~265 rows), small enough to ship to executors as a literal map. */
object Zones {

  val Schema: StructType = StructType(Seq(
    StructField("locationid", IntegerType),
    StructField("borough", StringType),
    StructField("zone", StringType),
    StructField("service_zone", StringType)))

  def parseCsv(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .csv(path)
      .select(Schema.fields.map(f => col(f.name).cast(f.dataType).as(f.name)): _*)

  def readMaster(spark: SparkSession, tables: MasterTables): DataFrame = spark.table(tables.zones)

  def toMap(zones: DataFrame): Map[Int, String] =
    zones
      .where(col("locationid").isNotNull && col("zone").isNotNull)
      .select("locationid", "zone")
      .collect()
      .map(r => r.getInt(0) -> r.getString(1))
      .toMap
}
