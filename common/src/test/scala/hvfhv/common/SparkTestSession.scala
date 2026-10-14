package hvfhv.common

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

/** One local SparkSession per test JVM, configured exactly like the production jobs. */
trait SparkTestSession {
  lazy val spark: SparkSession = SparkSessions
    .builder("hvfhv-test")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
}

object TripFixtures {

  val ZoneNames: Map[Int, String] = Map(
    132 -> "JFK Airport",
    161 -> "Midtown Center",
    230 -> "Times Sq/Theatre District")

  /** A fully populated Uber trip, JFK -> Midtown, requested 2022-01-02 08:00. */
  val Base: Map[String, String] = Map(
    "hvfhs_license_num" -> "HV0003",
    "dispatching_base_num" -> "B03404",
    "originating_base_num" -> "B03404",
    "request_datetime" -> "2022-01-02T08:00:00.000",
    "on_scene_datetime" -> "2022-01-02T08:03:00.000",
    "pickup_datetime" -> "2022-01-02T08:05:00.000",
    "dropoff_datetime" -> "2022-01-02T08:35:00.000",
    "pulocationid" -> "132",
    "dolocationid" -> "161",
    "trip_miles" -> "17.3",
    "trip_time" -> "1800",
    "base_passenger_fare" -> "50.10",
    "tolls" -> "6.55",
    "bcf" -> "1.50",
    "sales_tax" -> "4.44",
    "congestion_surcharge" -> "2.75",
    "airport_fee" -> "1.25",
    "tips" -> "5.00",
    "driver_pay" -> "40.00",
    "shared_request_flag" -> "N",
    "shared_match_flag" -> "N",
    "wav_request_flag" -> "N",
    "wav_match_flag" -> "N")

  /** `field -> null` removes the field, the way Socrata omits nulls from JSON. */
  def trip(overrides: (String, String)*): Map[String, String] =
    overrides.foldLeft(Base) {
      case (t, (k, null)) => t - k
      case (t, (k, v)) => t + (k -> v)
    }

  def json(trip: Map[String, String]): String =
    trip.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")

  /** A route (and optionally a request date), as identified in the batch and speed views. */
  final case class Route(carrier: String, pickup: String, dropoff: String, hour: Int, date: Option[String] = None) {
    def on(requestDate: String): Route = copy(date = Some(requestDate))
  }

  /** Aggregate rows ([[TripTransforms.aggregate]] shape) keyed by route (+ date when present). */
  def statsByRoute(aggregates: DataFrame): Map[Route, Map[String, Long]] = {
    val hasDate = aggregates.columns.contains(TripSchema.RequestDate)
    aggregates.collect().map { r =>
      Route(
        r.getAs[String](TripTransforms.Carrier),
        r.getAs[String](TripTransforms.PickupZone),
        r.getAs[String](TripTransforms.DropoffZone),
        r.getAs[Int](TripTransforms.HourInDay),
        if (hasDate) Some(r.getAs[String](TripSchema.RequestDate)) else None) ->
        Metrics.Columns.map(c => c -> r.getAs[Long](c)).toMap
    }.toMap
  }

  /** String-typed trips, as parsed from Kafka JSON by the speed layer. */
  def rawJson(spark: SparkSession, trips: Seq[Map[String, String]]): DataFrame =
    spark.read.schema(TripSchema.Raw).json(spark.createDataset(trips.map(json))(Encoders.STRING))
}
