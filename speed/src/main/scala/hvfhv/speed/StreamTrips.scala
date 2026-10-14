package hvfhv.speed

import hvfhv.common._
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Speed layer. Reads trip JSON from Kafka and runs two independent streaming queries,
 * each with its own checkpoint (so Kafka offsets survive restarts and nothing is skipped):
 *
 *  - archive: writes every trip to the landing zone (exactly-once file sink); the next
 *    batch run absorbs it into the Hive master dataset;
 *  - speed view: upserts each micro-batch's per-route, per-request-date totals to Cassandra.
 */
object StreamTrips {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val spark = SparkSessions.forJob("hvfhv-speed-layer", config)
    val keyspace = config.cassandra.keyspace

    CassandraIO.connector(spark).withSessionDo(CassandraIO.ensureServingSchema(_, keyspace))

    val zones = Zones.toMap(Zones.readMaster(spark, config.hive.tables))
    require(zones.nonEmpty, s"No taxi zones in ${config.hive.tables.zones}; run IngestMasterDataset first")

    val trips = readTrips(spark, config.kafka, config.speed)
    val trigger = Trigger.ProcessingTime(config.speed.triggerInterval.toMillis)
    val checkpoints = config.paths.checkpoints

    val archive = trips.writeStream
      .queryName("archive")
      .format("orc")
      .partitionBy(TripSchema.RequestDate)
      .option("path", config.paths.landingEvents)
      .option("checkpointLocation", s"$checkpoints/archive")
      .trigger(trigger)
      .start()

    val writer = new SpeedWriter(zones, new CassandraSpeedSink(keyspace))
    val speedView = trips.writeStream
      .queryName("speed-view")
      .foreachBatch(writer.asForeachBatch)
      .option("checkpointLocation", s"$checkpoints/speed-view")
      .trigger(trigger)
      .start()

    stopGracefullyOnShutdown(Seq(archive, speedView))
    log.info(s"Streaming ${config.kafka.topic} into $keyspace.${CassandraSchema.SpeedTable} and ${config.paths.landingEvents}")
    spark.streams.awaitAnyTermination()
  }

  /** Kafka JSON -> [[TripSchema.Canonical]], parsed by the same code as the batch layer. */
  def readTrips(spark: SparkSession, kafka: KafkaSettings, speed: SpeedSettings): DataFrame = {
    val messages = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka.brokers)
      .option("subscribe", kafka.topic)
      // Only applies to a brand-new checkpoint; afterwards the checkpointed offsets win.
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", speed.maxOffsetsPerTrigger.toString)
      .load()

    val parsed = messages
      .select(from_json(col("value").cast("string"), TripSchema.Raw).as("trip"))
      .where(col("trip").isNotNull)
      .select("trip.*")
    TripSchema.normalize(parsed)
  }

  /**
   * StreamingQuery.stop() interrupts a running micro-batch. That is safe (the batch is
   * replayed and rewrites the same rows), but finishing it first avoids redoing the work.
   */
  private def stopGracefullyOnShutdown(queries: Seq[StreamingQuery]): Unit =
    sys.addShutdownHook {
      log.info("Shutting down streaming queries after their in-flight micro-batches")
      queries.foreach { q =>
        while (q.isActive && q.status.isTriggerActive) Thread.sleep(100)
        q.stop()
      }
    }
}
