import DataTypeConverters._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Increment, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.util.Try


object StreamTrips {

  // Configure a JSON ObjectMapper for parsing JSON data
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // Set up the HBase configuration, specifying the ZooKeeper client port and quorum
  val hbaseConf: Configuration = HBaseConfiguration.create()

  // Establish connections to HBase tables
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val hvfhs_route_hourly_summary_speed = hbaseConnection.getTable(TableName.valueOf("jycchien_hvfhs_route_hourly_summary_speed"))
  val zoneTable = hbaseConnection.getTable(TableName.valueOf("jycchien_zone_map"))
  val licenseTable = hbaseConnection.getTable(TableName.valueOf("jycchien_license"))

  def convertToLong(congestionSurcharge: Option[String]): Option[Long] = {
    congestionSurcharge.flatMap { surchargeStr =>
      Try(surchargeStr.toDouble.toLong).toOption
    }
  }


  // The main method checks for command-line arguments and prints usage information if insufficient arguments are provided
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamTrips <brokers>
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    // Extract the Kafka brokers from the command-line arguments.
    val Array(brokers) = args

    // Create a Spark configuration and a StreamingContext with a 10-second batch interval
    val sparkConf = new SparkConf().setAppName("StreamTraffic")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Define the Kafka topic and parameters for the Kafka consumer
    val topicsSet = Set("jycchien_hvfhv")
    val kafkaParams = Map[String, Object](
      // Specify the list of Kafka broker addresses to connect to
      // The brokers variable contains the broker addresses passed as a command-line argument
      "bootstrap.servers" -> brokers,
      // Set the deserializer for the Kafka message keys
      "key.deserializer" -> classOf[StringDeserializer],
      // Set the deserializer for the Kafka message values
      "value.deserializer" -> classOf[StringDeserializer],
      // Assigns a unique consumer group ID for this Kafka consumer and for each stream to ensure independent consumption
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      // Determine where to start reading messages when no committed offset is found
      // "latest" means it will start consuming from the most recent messages
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // Create direct kafka stream with brokers and topic
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )
    // Extract the value from each Kafka record and deserialize it into a KafkaTripRecord object.
    val serializedRecords = stream.map(_.value);
    val ktr = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaTripRecord]))


    val batchStats = ktr.map ( tr =>
      // Check if required fields are defined
      if (tr.hvfhsLicenseNum.isDefined &&
          tr.pickupDatetime.isDefined &&
          tr.requestDatetime.isDefined &&
          tr.puLocationId.isDefined &&
          tr.doLocationId.isDefined &&
          tr.basePassengerFare.isDefined) {

        // Read current values from HBase
        val getCarrier = new Get(Bytes.toBytes(tr.hvfhsLicenseNum.get))
        getCarrier.addColumn(Bytes.toBytes("info"), Bytes.toBytes("carrier"))
        val license = licenseTable.get(getCarrier)
        val carrierValue = Bytes.toString(license.getValue(Bytes.toBytes("info"), Bytes.toBytes("carrier")))

        val getPickupZone = new Get(Bytes.toBytes(tr.puLocationId.get))
        getPickupZone.addColumn(Bytes.toBytes("info"), Bytes.toBytes("zone"))
        val pickupZoneResult = zoneTable.get(getPickupZone)
        val pickupZoneName = Bytes.toString(pickupZoneResult.getValue(Bytes.toBytes("info"), Bytes.toBytes("zone")))

        val getDropoffZone = new Get(Bytes.toBytes(tr.doLocationId.get))
        getDropoffZone.addColumn(Bytes.toBytes("info"), Bytes.toBytes("zone"))
        val dropoffZoneResult = zoneTable.get(getPickupZone)
        val dropoffZoneName = Bytes.toString(dropoffZoneResult.getValue(Bytes.toBytes("info"), Bytes.toBytes("zone")))


        val pickupTimestamp = stringToTimestamp(tr.pickupDatetime)
        val hourInDay = getHourFromTimestamp(pickupTimestamp)

        val hourlyRoute = s"$carrierValue|$pickupZoneName|$dropoffZoneName|$hourInDay"
        // Prepare increment operation
        val increment = new Increment(Bytes.toBytes(hourlyRoute))

        // Update stats with safe handling of optional fields
        increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("congestion_surcharge_count"), 1L)
        increment.addColumn(
          Bytes.toBytes("stats"),
          Bytes.toBytes("total_congestion_surcharge"),
          getDoubleOrDefault(stringToDouble(tr.congestionSurcharge)).toLong
        )
        increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("tolls_count"), 1L)
        increment.addColumn(
          Bytes.toBytes("stats"),
          Bytes.toBytes("total_tolls"),
          getDoubleOrDefault(stringToDouble(tr.tolls)).toLong
        )
        increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("revenue_count"), 1L)
        increment.addColumn(
          Bytes.toBytes("stats"),
          Bytes.toBytes("total_revenue"),
          (getDoubleOrDefault(stringToDouble(tr.basePassengerFare)) +
            getDoubleOrDefault(stringToDouble(tr.congestionSurcharge)) +
            getDoubleOrDefault(stringToDouble(tr.tolls)) +
            getDoubleOrDefault(stringToDouble(tr.bcf)) +
            getDoubleOrDefault(stringToDouble(tr.salesTax)) +
            getDoubleOrDefault(stringToDouble(tr.airportFee)) +
            getDoubleOrDefault(stringToDouble(tr.tips))
            ).toLong
        )
        increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("trip_time_count"), 1L)
        increment.addColumn(
          Bytes.toBytes("stats"),
          Bytes.toBytes("total_trip_time"),
          stringToLong(tr.tripTime).getOrElse(0L)
        )
        increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("wait_time_count"), 1L)
        increment.addColumn(
          Bytes.toBytes("stats"),
          Bytes.toBytes("total_wait_time"),
          getTimestampMillis(stringToTimestamp(tr.pickupDatetime)) - getTimestampMillis(stringToTimestamp(tr.requestDatetime))
        )

        // Perform HBase increment operation
        hvfhs_route_hourly_summary_speed.increment(increment)

        println(s"Processed hourly route: $hourlyRoute")
      }
    )


    batchStats.print()

    ssc.start()
    ssc.awaitTermination()
  }

}