package hvfhv.common

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class ContractSpec extends AnyFunSuite {

  test("config resolves paths from the base directory and honours overrides") {
    val config = AppConfig.fromConfig(
      ConfigFactory.parseString("hvfhv.paths.base = /mnt/x, hvfhv.hive.database = demo, hvfhv.cassandra.keyspace = demo_ks")
        .withFallback(ConfigFactory.parseResources("reference.conf")).resolve())
    assert(config.paths.landingEvents == "/mnt/x/landing/events")
    assert(config.hive.tables.bulk == "demo.trips_bulk")
    assert(config.hive.tables.stream == "demo.trips_stream")
    assert(config.cassandra.keyspace == "demo_ks")
  }

  test("batch view tables are recognised by name, so cleanup never drops other tables") {
    assert(CassandraSchema.batchView(42) == "route_hourly_v42")
    assert(CassandraSchema.isBatchView("route_hourly_v42"))
    assert(!CassandraSchema.isBatchView("speed"))
    assert(!CassandraSchema.isBatchView("route_hourly_v42_backup"))
  }

  test("the speed table is partitioned by route and clustered by date, query and micro-batch") {
    val ddl = CassandraSchema.createSpeed("ks")
    assert(ddl.contains("PRIMARY KEY ((carrier, pickup_zone, dropoff_zone, hour_in_day), request_date, query_id, batch_id))"))
    Metrics.Columns.foreach(c => assert(ddl.contains(s"$c bigint"), c))
    assert(CassandraSchema.deleteSpeedBefore("ks") ==
      "DELETE FROM ks.speed WHERE carrier = ? AND pickup_zone = ? AND dropoff_zone = ? AND hour_in_day = ? AND request_date < ?")
  }
}
