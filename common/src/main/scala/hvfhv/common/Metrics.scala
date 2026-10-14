package hvfhv.common

/**
 * Metrics served per hourly route. Each is stored as a `<name>_total` / `<name>_count`
 * pair so that batch and speed rows can be added together before dividing. Counts only
 * include trips where the metric is known, so a missing value never drags an average to 0.
 *
 * Units are fixed integers so the speed layer can use HBase `Increment`:
 * money in cents, durations in seconds.
 */
object Metrics {

  final case class Metric(name: String) {
    val total: String = s"${name}_total"
    val count: String = s"${name}_count"
    /** Per-trip value column produced by [[TripTransforms.enrich]]. */
    val value: String = s"${name}_value"
  }

  val CongestionSurcharge: Metric = Metric("congestion_surcharge") // cents
  val Tolls: Metric = Metric("tolls") // cents
  val Revenue: Metric = Metric("revenue") // cents
  val TripTime: Metric = Metric("trip_time") // seconds
  val WaitTime: Metric = Metric("wait_time") // seconds

  val All: Seq[Metric] = Seq(CongestionSurcharge, Tolls, Revenue, TripTime, WaitTime)

  /** Number of trips on the route, whether or not their metric values are known. */
  val TripCount: String = "trip_count"

  /** HBase qualifiers (and aggregate DataFrame columns), in a stable order. */
  val Columns: Seq[String] = All.flatMap(m => Seq(m.total, m.count)) :+ TripCount
}
