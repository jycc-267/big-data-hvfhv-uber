'use strict';

const cassandra = require('cassandra-driver');
const { ROUTE_COLUMNS, TABLES, METRIC_COLUMNS, assertBatchTable } = require('./schema');

const routeWhere = ROUTE_COLUMNS.map((c) => `${c} = ?`).join(' AND ');
const routeParams = ({ carrier, pickup, dropoff, hour }) => [carrier, pickup, dropoff, hour];

/** Cassandra bigints arrive as driver Long objects (64-bit); totals here fit in a Number. */
function toStats(row) {
  if (!row) return null;
  const stats = {};
  for (const column of METRIC_COLUMNS) {
    const value = row[column];
    if (value !== null && value !== undefined) stats[column] = Number(value.toString());
  }
  return stats;
}

/** The serving-layer queries the app needs, over the Cassandra Node.js driver. */
function createCassandraStore({ contactPoints, localDataCenter, keyspace }) {
  const client = new cassandra.Client({ contactPoints, localDataCenter, keyspace });
  const run = (query, params) => client.execute(query, params, { prepare: true });

  return {
    /** The active batch view and its watermark, or null before the first batch run. */
    async pointer() {
      const result = await run(
        `SELECT batch_table, watermark, built_at FROM ${TABLES.pointer} WHERE name = ?`, ['serving']);
      const row = result.first();
      return row ? { batchTable: row.batch_table, watermark: row.watermark, builtAt: row.built_at } : null;
    },

    /** One route's row in a batch view, or null when the route had no trips. */
    async batchStats(table, route) {
      const result = await run(
        `SELECT ${METRIC_COLUMNS.join(', ')} FROM ${assertBatchTable(table)} WHERE ${routeWhere}`,
        routeParams(route));
      return toStats(result.first());
    },

    /** One route's speed rows dated on or after `watermark`, summed (a single-partition query). */
    async speedStats(route, watermark) {
      const sums = METRIC_COLUMNS.map((c) => `sum(${c}) AS ${c}`).join(', ');
      const result = await run(
        `SELECT ${sums} FROM ${TABLES.speed} WHERE ${routeWhere} AND request_date >= ?`,
        [...routeParams(route), watermark]);
      return toStats(result.first()) || {};
    },

    async lookups(kind) {
      const result = await run(`SELECT value FROM ${TABLES.lookups} WHERE kind = ?`, [kind]);
      return result.rows.map((row) => row.value);
    },

    shutdown: () => client.shutdown(),
  };
}

module.exports = { createCassandraStore, toStats };
