package hvfhv.common

/**
 * The batch layer's master dataset: the ground truth every view can be rebuilt from.
 * Hive owns these tables (deploy/hive/master_tables.hql creates them); Spark reads and
 * writes them through the Hive Metastore.
 *
 *  - trips_bulk:   the historical bulk download, replaced as a whole on re-ingest
 *  - trips_stream: trips that arrived through the speed layer, absorbed by each batch run
 *  - zones:        taxi zone lookup
 *
 * Both trip tables hold [[TripSchema.Canonical]] records partitioned by request date.
 */
final case class MasterTables(database: String) {
  val bulk: String = s"$database.trips_bulk"
  val stream: String = s"$database.trips_stream"
  val zones: String = s"$database.zones"
}
