
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
                                                   unix_timestamp(col("pickup_datetime")) - unix_timestamp(col("request_datetime")))

hvfhs_trip.createOrReplaceTempView("hvfhs_trip")
hvfhs_trip.write.
    format("parquet").
    mode("append").
    option("path", "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/jycchien/hvfhs_trip").
    saveAsTable("jycchien_hvfhs_trip")



import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

// Set up the HBase configuration, specifying the ZooKeeper client port and quorum
val hbaseConf = HBaseConfiguration.create()

  // Establish connections to HBase tables
val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
val zone_map = hbaseConnection.getTable(TableName.valueOf("jycchien_zone_map"))
val zone = hbaseConnection.getTable(TableName.valueOf("jycchien_zone"))
val hours = hbaseConnection.getTable(TableName.valueOf("jycchien_hours"))


// Fetch the required columns from the Hive table
val mappings = taxi_zone_lookup.select("locationid", "zone").collect()

// Write data to HBase
mappings.foreach { row =>
  val locationId = row.getString(0)
  val zoneName = row.getString(1)
  
  val put = new Put(Bytes.toBytes(locationId))
  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("zone"), Bytes.toBytes(zoneName))
  
  zone_map.put(put)
}

val zone_list = taxi_zone_lookup.select("zone").collect()
zone_list.foreach { row =>
  val zoneName = row.getString(0)
  
  val put = new Put(Bytes.toBytes(zoneName))
  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("zone"), Bytes.toBytes(zoneName))
  
  zone.put(put)
}

val hour_list = (0 to 23).toList
hour_list.foreach { hour =>
  val rowKey = Bytes.toBytes(hour.toString)
  val put = new Put(rowKey)
  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hour"), Bytes.toBytes(hour.toString))
  
  hours.put(put)
}

// Close the HBase connection
hbaseConnection.close()









