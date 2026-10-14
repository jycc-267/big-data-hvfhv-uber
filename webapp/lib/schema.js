'use strict';

// Mirrors common/src/main/scala/hvfhv/common/{CassandraSchema,Metrics,TripTransforms}.scala.
// Keep the two in sync: this is the contract between the pipeline and the web app.

const ROUTE_COLUMNS = ['carrier', 'pickup_zone', 'dropoff_zone', 'hour_in_day'];
const TRIP_COUNT = 'trip_count';

const TABLES = {
  speed: 'speed',
  pointer: 'serving_pointer',
  lookups: 'lookups',
};

/** Stored as integer totals; `scale` converts a per-trip average to display units. */
const METRICS = [
  { name: 'congestion_surcharge', label: 'Congestion Surcharge ($)', scale: 100 }, // cents
  { name: 'tolls', label: 'Tolls ($)', scale: 100 }, // cents
  { name: 'revenue', label: 'Price ($)', scale: 100 }, // cents
  { name: 'trip_time', label: 'Trip Time (minutes)', scale: 60 }, // seconds
  { name: 'wait_time', label: 'Wait Time (minutes)', scale: 60 }, // seconds
];

/** Every stats column, in the order the pipeline writes them. */
const METRIC_COLUMNS = [...METRICS.flatMap(({ name }) => [`${name}_total`, `${name}_count`]), TRIP_COUNT];

/**
 * Batch views are named route_hourly_v<timestamp>. The name comes from the database and
 * can't be a bound CQL parameter, so it must match this pattern before it is used.
 */
const BATCH_TABLE = /^route_hourly_v\d+$/;

function assertBatchTable(name) {
  if (!BATCH_TABLE.test(name)) throw new Error(`Refusing to query unexpected table name: ${name}`);
  return name;
}

module.exports = { ROUTE_COLUMNS, TRIP_COUNT, TABLES, METRICS, METRIC_COLUMNS, assertBatchTable };
