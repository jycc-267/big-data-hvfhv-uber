
// In spark-shell
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._ // used for column operations
import org.apache.spark.sql.types._

import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR

val hive = HiveWarehouseSession.session(spark).build()
hive.setDatabase("default")


// Retrieve the base HDFS path from Hadoop configuration
// wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/tmp/jycchien/fhvdata
val basePath = sc.hadoopConfiguration.get("fs.defaultFS")

/**
 * Hive will give column names like route_delays.origin instead of just origin if we read the table directly
 * This will give us problems down the road
 * 
 * Write a loadHiveTable() function to remove table name prefix and load hive table as Spark DataFrame
 */
def loadHiveTable(tableName: String): DataFrame = {

  val prefix = s"$tableName."

  // Load the table into a DataFrame
  // Note dot positioning (so that the spark shell knows the line isn't complete)
  var df = spark.read.format("jdbc"). // uses JDBC to connect to Hive
                      option("url", "jdbc:hive2://10.0.0.50:10001/;transportMode=http"). // specify the connection URL
                      option("dbtable", tableName).load() // specify table name to read it as dataframe

  // Remove the prefix from each column name by renaming columns explicitly
  df.columns.foreach(colName =>
    if (colName.startsWith(prefix)) {
      df = df.withColumnRenamed(colName, colName.stripPrefix(prefix))
    }
  )

  // Return the flattened DataFrame
  df
}

val fhv_tripdata = loadHiveTable("jycchien_fhv_tripdata")
val taxi_zone_lookup = loadHiveTable("jycchien_zone_lookup")

fhv_tripdata.createOrReplaceTempView("fhv_tripdata")
taxi_zone_lookup.createOrReplaceTempView("taxi_zone_lookup")


val fhv_tripdata_with_zone = fhv_tripdata.join(taxi_zone_lookup.alias("pickup"),
                                               fhv_tripdata("pulocationid") === col("pickup.locationid"),
                                               "left").
                                          join(taxi_zone_lookup.alias("dropoff"),
                                               fhv_tripdata("dolocationid") === col("dropoff.locationid"),
                                               "left").
                                          select(fhv_tripdata("*"),
                                                 col("pickup.borough").as("pickup_borough"),
                                                 col("pickup.zone").as("pickup_zone"),
                                                 col("pickup.service_zone").as("pickup_service_zone"),
                                                 col("dropoff.borough").as("dropoff_borough"),
                                                 col("dropoff.zone").as("dropoff_zone"),
                                                 col("dropoff.service_zone").as("dropoff_service_zone"))

// Show the first row of the joined data
fhv_tripdata_with_zone.show(1)

// Create new columns and calculate required metrics
val hvfhs_trip = fhv_tripdata_with_zone.withColumn("hour_in_day", hour(col("pickup_datetime"))).
                                        withColumn("revenue", col("base_passenger_fare") + col("tolls") + col("bcf") + 
                                                              col("sales_tax") + col("congestion_surcharge") + 
                                                              col("airport_fee") + col("tips")).
                                        withColumn("wait_time", 
                                                   unix_timestamp(col("on_scene_datetime")) - unix_timestamp(col("request_datetime")))

hvfhs_trip.createOrReplaceTempView("hvfhs_trip")
hvfhs_trip.write.
    format("parquet").
    mode("append").
    option("path", "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/jycchien/hvfhs_trip").
    saveAsTable("jycchien_hvfhs_trip")


val hvfhs_route_hourly = hvfhs_trip.groupBy(
  col("hvfhs_license_num"),
  col("pickup_zone"),
  col("dropoff_zone"),
  col("hour_in_day")
).agg(
  sum(when(col("congestion_surcharge").isNotNull, 1).otherwise(0)).as("congestion_surcharge_count"),
  sum(col("congestion_surcharge")).as("total_congestion_surcharge"),
  sum(when(col("tolls").isNotNull, 1).otherwise(0)).as("tolls_count"),
  sum(col("tolls")).as("total_tolls"),
  sum(when(col("revenue").isNotNull, 1).otherwise(0)).as("revenue_count"),
  sum(col("revenue")).as("total_revenue"),
  sum(when(col("trip_time").isNotNull, 1).otherwise(0)).as("trip_time_count"),
  sum(col("trip_time")).as("total_trip_time"),
  sum(when(col("wait_time").isNotNull, 1).otherwise(0)).as("wait_time_count"),
  sum(col("wait_time")).as("total_wait_time")
)


// val hvfhs_route_hourly_summary = hvfhs_route_hourly.select(
//   col("hvfhs_license_num"),
//   col("pickup_zone"),
//   col("dropoff_zone"),
//   col("hour_in_day"),
//   when(col("congestion_surcharge_count") === 0, null)
//     .otherwise(col("total_congestion_surcharge") / col("congestion_surcharge_count")).as("avg_congestion_surcharge"),
//   when(col("tolls_count") === 0, null)
//     .otherwise(col("total_tolls") / col("tolls_count")).as("avg_tolls"),
//   when(col("revenue_count") === 0, null)
//     .otherwise(col("total_revenue") / col("revenue_count")).as("avg_revenue"),
//   when(col("trip_time_count") === 0, null)
//     .otherwise(col("total_trip_time") / col("trip_time_count")).as("avg_trip_time"),
//   when(col("wait_time_count") === 0, null)
//     .otherwise(col("total_wait_time") / col("wait_time_count")).as("avg_wait_time")
// )

hvfhs_route_hourly_summary.createOrReplaceTempView("hvfhs_route_hourly_summary")
hvfhs_route_hourly_summary.write.
    format("parquet").
    mode("append").
    option("path", "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/jycchien/hvfhs_route_hourly_summary").
    saveAsTable("jycchien_hvfhs_route_hourly")