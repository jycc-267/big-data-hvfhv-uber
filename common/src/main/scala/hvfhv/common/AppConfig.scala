package hvfhv.common

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}

final case class Paths(
    rawTrips: String,
    rawZones: String,
    landingEvents: String,
    staging: String,
    checkpoints: String)

final case class HiveSettings(database: String, metastoreUri: String) {
  val tables: MasterTables = MasterTables(database)
}

final case class CassandraSettings(host: String, port: Int, keyspace: String)

final case class KafkaSettings(brokers: String, topic: String)

final case class SpeedSettings(triggerInterval: FiniteDuration, maxOffsetsPerTrigger: Long)

final case class AppConfig(
    paths: Paths,
    hive: HiveSettings,
    cassandra: CassandraSettings,
    kafka: KafkaSettings,
    speed: SpeedSettings)

object AppConfig {

  def load(): AppConfig = fromConfig(ConfigFactory.load())

  def fromConfig(root: Config): AppConfig = {
    val c = root.getConfig("hvfhv")
    val p = c.getConfig("paths")
    AppConfig(
      paths = Paths(
        rawTrips = p.getString("raw-trips"),
        rawZones = p.getString("raw-zones"),
        landingEvents = p.getString("landing-events"),
        staging = p.getString("staging"),
        checkpoints = p.getString("checkpoints")),
      hive = HiveSettings(c.getString("hive.database"), c.getString("hive.metastore-uri")),
      cassandra = CassandraSettings(c.getString("cassandra.host"), c.getInt("cassandra.port"), c.getString("cassandra.keyspace")),
      kafka = KafkaSettings(c.getString("kafka.brokers"), c.getString("kafka.topic")),
      speed = SpeedSettings(
        triggerInterval = Duration(c.getDuration("speed.trigger-interval").toMillis, MILLISECONDS),
        maxOffsetsPerTrigger = c.getLong("speed.max-offsets-per-trigger")))
  }
}
