package hvfhv.common

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant

import hvfhv.common.Metrics._
import hvfhv.common.TripFixtures._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite

class TripTransformsSpec extends AnyFunSuite with SparkTestSession {

  private def normalized(trips: Map[String, String]*): DataFrame = TripSchema.normalize(rawJson(spark, trips))

  private def enriched(trips: Map[String, String]*): DataFrame = TripTransforms.enrich(normalized(trips: _*), ZoneNames)

  private def onlyRow(df: DataFrame): Row = {
    val rows = df.collect()
    assert(rows.length == 1, s"expected one row, got ${rows.mkString(", ")}")
    rows.head
  }

  private val jfkMidtown8 = Route("Uber", "JFK Airport", "Midtown Center", 8)

  test("normalize parses ISO and US timestamps, exact money and the request date") {
    val row = onlyRow(normalized(
      trip("request_datetime" -> "01/02/2022 11:59:30 PM", "tolls" -> "6.55", "access_a_ride_flag" -> " ")))
    // The UTC session stores the wall-clock time as the UTC instant.
    assert(row.getAs[Timestamp]("request_datetime").toInstant == Instant.parse("2022-01-02T23:59:30Z"))
    assert(row.getAs[Timestamp]("pickup_datetime").toInstant == Instant.parse("2022-01-02T08:05:00Z"))
    assert(row.getAs[java.math.BigDecimal]("tolls") == new java.math.BigDecimal("6.55"))
    assert(row.getAs[String](TripSchema.RequestDate) == "20220102")
    assert(row.isNullAt(row.fieldIndex("access_a_ride_flag")), "blank strings become null")
  }

  test("normalize turns unparseable values into nulls instead of failing") {
    val row = onlyRow(normalized(trip("pickup_datetime" -> "not a time", "trip_time" -> "abc")))
    assert(row.isNullAt(row.fieldIndex("pickup_datetime")))
    assert(row.isNullAt(row.fieldIndex("trip_time")))
  }

  test("enrich resolves pickup and dropoff zones independently (regression: dropoff used pickup id)") {
    val row = onlyRow(enriched(trip("pulocationid" -> "132", "dolocationid" -> "230")))
    assert(row.getAs[String](TripTransforms.PickupZone) == "JFK Airport")
    assert(row.getAs[String](TripTransforms.DropoffZone) == "Times Sq/Theatre District")
  }

  test("enrich maps license numbers to carriers and keeps unknown licenses as-is") {
    val carriers = enriched(trip("hvfhs_license_num" -> "HV0005"), trip("hvfhs_license_num" -> "HV0099"))
      .select(TripTransforms.Carrier).collect().map(_.getString(0)).toSet
    assert(carriers == Set("Lyft", "HV0099"))
  }

  test("enrich drops trips that cannot be placed on a route instead of inventing a key") {
    val unplaceable = Seq(
      trip("dolocationid" -> "7"), // unknown zone
      trip("hvfhs_license_num" -> null),
      trip("pickup_datetime" -> null), // no hour; the old speed layer used hour 0
      trip("request_datetime" -> "garbage"))
    assert(enriched(unplaceable: _*).count() == 0)
  }

  test("metrics are exact cents and seconds") {
    val row = onlyRow(enriched(Base))
    assert(row.getAs[Long](CongestionSurcharge.value) == 275L) // the old speed layer stored 2
    assert(row.getAs[Long](Tolls.value) == 655L)
    assert(row.getAs[Long](Revenue.value) == 5010L + 655 + 150 + 444 + 275 + 125 + 500)
    assert(row.getAs[Long](TripTime.value) == 1800L)
    assert(row.getAs[Long](WaitTime.value) == 300L) // seconds, not milliseconds
  }

  test("revenue treats missing optional components as zero but needs a base fare") {
    val noAirportFee = onlyRow(enriched(trip("airport_fee" -> null)))
    assert(noAirportFee.getAs[Long](Revenue.value) == 5010L + 655 + 150 + 444 + 275 + 500)

    val noBaseFare = onlyRow(enriched(trip("base_passenger_fare" -> null)))
    assert(noBaseFare.isNullAt(noBaseFare.fieldIndex(Revenue.value)))
  }

  test("negative wait times (pickup before request) are unknown, not negative") {
    val row = onlyRow(enriched(trip("pickup_datetime" -> "2022-01-02T07:50:00.000")))
    assert(row.isNullAt(row.fieldIndex(WaitTime.value)))
  }

  test("hour_in_day is the wall-clock hour even inside a DST gap") {
    // 02:30 on 2022-03-13 does not exist in New York; it must still land in hour 2.
    val row = onlyRow(enriched(trip("pickup_datetime" -> "2022-03-13T02:30:00.000", "request_datetime" -> "2022-03-13T02:20:00.000")))
    assert(row.getAs[Int](TripTransforms.HourInDay) == 2)
  }

  test("aggregate counts only known values, so missing data does not bias averages") {
    val stats = statsByRoute(TripTransforms.aggregate(
      enriched(Base, trip("tolls" -> null), trip("tolls" -> "1.00")), byRequestDate = false))
    val key = jfkMidtown8
    assert(stats.keySet == Set(key))
    assert(stats(key)(Tolls.count) == 2L)
    assert(stats(key)(Tolls.total) == 755L)
    assert(stats(key)(TripTime.count) == 3L)
    assert(stats(key)(TripCount) == 3L)
  }

  test("aggregate by request date produces one speed-view row per route and date") {
    val stats = statsByRoute(TripTransforms.aggregate(
      enriched(Base, trip("request_datetime" -> "2022-01-03T07:59:00.000", "pickup_datetime" -> "2022-01-03T08:01:00.000")),
      byRequestDate = true))
    assert(stats.keySet == Set(jfkMidtown8.on("20220102"), jfkMidtown8.on("20220103")))
  }

  test("CSV (batch) and JSON (speed) inputs produce identical aggregates") {
    val trips = Seq(Base, trip("tolls" -> null, "hvfhs_license_num" -> "HV0002"), trip("dolocationid" -> "230", "tips" -> "0.0"))

    val dir = Files.createTempDirectory("trips-csv").toString
    rawJson(spark, trips).write.mode("overwrite").option("header", "true").csv(dir)
    val fromCsv = TripSchema.normalize(spark.read.option("header", "true").csv(dir))
    val fromJson = TripSchema.normalize(rawJson(spark, trips))

    def view(df: DataFrame) = statsByRoute(TripTransforms.aggregate(TripTransforms.enrich(df, ZoneNames), byRequestDate = false))
    assert(view(fromCsv) == view(fromJson))
    assert(view(fromJson).size == 3)
  }
}
