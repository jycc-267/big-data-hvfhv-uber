package hvfhv.common

import hvfhv.common.Metrics._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame}

/**
 * The only definition of how a trip maps to a route and to metric values. The batch job
 * runs it over the master dataset and the speed job runs it over every micro-batch, so
 * batch and speed rows are always computed the same way and can be summed by readers.
 */
object TripTransforms {

  val Carrier: String = "carrier"
  val PickupZone: String = "pickup_zone"
  val DropoffZone: String = "dropoff_zone"
  val HourInDay: String = "hour_in_day"

  /**
   * Adds route dimensions and per-trip metric values to [[TripSchema.Canonical]] records.
   * Trips that can't be placed on a route (unknown zone, no license, no pickup or request
   * time) are dropped. Unknown license numbers are kept under their raw value.
   *
   * @param zones taxi zone LocationID -> zone name
   */
  def enrich(trips: DataFrame, zones: Map[Int, String]): DataFrame = {
    val zoneName = typedLit(zones)
    val carrierName = typedLit(Carriers.ByLicense)
    val license = col("hvfhs_license_num")

    val routed = trips
      .withColumn(Carrier, coalesce(carrierName(license), license))
      .withColumn(PickupZone, zoneName(col("pulocationid")))
      .withColumn(DropoffZone, zoneName(col("dolocationid")))
      .withColumn(HourInDay, hour(col("pickup_datetime")))

    val placeable = Seq(Carrier, PickupZone, DropoffZone, HourInDay, TripSchema.RequestDate)
      .map(c => col(c).isNotNull)
      .reduce(_ && _)

    routed
      .where(placeable)
      .withColumn(CongestionSurcharge.value, cents(col("congestion_surcharge")))
      .withColumn(Tolls.value, cents(col("tolls")))
      .withColumn(Revenue.value, cents(revenue))
      .withColumn(TripTime.value, nonNegative(col("trip_time")))
      .withColumn(WaitTime.value, nonNegative(col("pickup_datetime").cast(LongType) - col("request_datetime").cast(LongType)))
  }

  /** The columns that identify a route; the batch view has one row per route. */
  val RouteColumns: Seq[String] = Seq(Carrier, PickupZone, DropoffZone, HourInDay)

  /**
   * Sums every metric per route (and per request date when `byRequestDate`, which is the
   * shape the speed view needs). Output: [[RouteColumns]] (+ request date) plus one
   * column per [[Metrics.Columns]].
   */
  def aggregate(enriched: DataFrame, byRequestDate: Boolean): DataFrame = {
    val keys = RouteColumns ++ (if (byRequestDate) Seq(TripSchema.RequestDate) else Nil)
    val sums = Metrics.All.flatMap { m =>
      Seq(coalesce(sum(col(m.value)), lit(0L)).as(m.total), count(col(m.value)).as(m.count))
    } :+ count(lit(1)).as(Metrics.TripCount)
    enriched
      .groupBy(keys.map(col): _*)
      .agg(sums.head, sums.tail: _*)
  }

  /** Fare components are optional; the trip has revenue only if its base fare is known. */
  private def revenue: Column = {
    val extras = Seq("tolls", "bcf", "sales_tax", "congestion_surcharge", "airport_fee", "tips")
      .map(c => coalesce(col(c), lit(0)))
    when(col("base_passenger_fare").isNotNull, extras.foldLeft(col("base_passenger_fare"))(_ + _))
  }

  /** Money columns are exact decimals, so x * 100 is a whole number of cents. */
  private def cents(money: Column): Column = (money * 100).cast(LongType)

  private def nonNegative(c: Column): Column = when(c >= 0, c)
}
