package hvfhv.common

import hvfhv.common.TripTransforms.{Carrier, DropoffZone, HourInDay, PickupZone, RouteColumns}

/**
 * The serving-layer contract shared by the batch job, the speed job and the web app
 * (webapp/lib/schema.js mirrors these names; keep the two in sync).
 *
 *  - `route_hourly_v<ts>`: one batch view per batch run, one row per route
 *    (carrier, pickup zone, dropoff zone, hour of day).
 *  - `speed`: one row per route, request date and speed-layer micro-batch. A replayed
 *    micro-batch rewrites the same rows with the same values, so replays are harmless.
 *  - `serving_pointer`: row `serving` names the active batch view and its watermark.
 *    It is a single-partition write, which Cassandra applies atomically.
 *  - `lookups`: values for the web app's drop-downs.
 *
 * Metric columns are [[Metrics.Columns]] (bigint; cents and seconds).
 */
object CassandraSchema {

  val SpeedTable: String = "speed"
  val PointerTable: String = "serving_pointer"
  val LookupsTable: String = "lookups"

  val BatchViewPrefix: String = "route_hourly_v"
  def batchView(version: Long): String = s"$BatchViewPrefix$version"
  def isBatchView(table: String): Boolean = table.matches(s"$BatchViewPrefix\\d+")

  object Speed {
    val RequestDate: String = TripSchema.RequestDate
    val QueryId = "query_id"
    val BatchId = "batch_id"
  }

  object Pointer {
    val Row = "serving"
    val BatchTable = "batch_table"
    val Watermark = "watermark"
    val BuiltAt = "built_at"
  }

  object Lookups {
    val Kind = "kind"
    val Value = "value"
    val Carrier = "carrier"
    val Zone = "zone"
    val Hour = "hour"
  }

  private val routeColumnDefs =
    s"$Carrier text, $PickupZone text, $DropoffZone text, $HourInDay int"
  private val metricColumnDefs = Metrics.Columns.map(c => s"$c bigint").mkString(", ")
  private val routeKey = RouteColumns.mkString(", ")

  def createKeyspace(keyspace: String): String =
    s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"

  def createBatchView(keyspace: String, table: String): String =
    s"CREATE TABLE IF NOT EXISTS $keyspace.$table ($routeColumnDefs, $metricColumnDefs, PRIMARY KEY (($routeKey)))"

  def createSpeed(keyspace: String): String =
    s"CREATE TABLE IF NOT EXISTS $keyspace.$SpeedTable ($routeColumnDefs, ${Speed.RequestDate} text, " +
      s"${Speed.QueryId} text, ${Speed.BatchId} bigint, $metricColumnDefs, " +
      s"PRIMARY KEY (($routeKey), ${Speed.RequestDate}, ${Speed.QueryId}, ${Speed.BatchId}))"

  def createPointer(keyspace: String): String =
    s"CREATE TABLE IF NOT EXISTS $keyspace.$PointerTable (name text PRIMARY KEY, " +
      s"${Pointer.BatchTable} text, ${Pointer.Watermark} text, ${Pointer.BuiltAt} text)"

  def createLookups(keyspace: String): String =
    s"CREATE TABLE IF NOT EXISTS $keyspace.$LookupsTable (${Lookups.Kind} text, ${Lookups.Value} text, " +
      s"PRIMARY KEY (${Lookups.Kind}, ${Lookups.Value}))"

  /** Deletes one route's speed rows dated before a cutoff (a single-partition range delete). */
  def deleteSpeedBefore(keyspace: String): String =
    s"DELETE FROM $keyspace.$SpeedTable WHERE " + RouteColumns.map(c => s"$c = ?").mkString(" AND ") +
      s" AND ${Speed.RequestDate} < ?"
}
