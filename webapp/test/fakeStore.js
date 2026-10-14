'use strict';

const { assertBatchTable } = require('../lib/schema');

const routeKey = ({ carrier, pickup, dropoff, hour }) => [carrier, pickup, dropoff, hour].join('/');

/**
 * In-memory stand-in for lib/cassandraStore.js with the same semantics:
 *  - pointer: the `serving` row, or null
 *  - batchStats: one route's row in a batch table, or null
 *  - speedStats: one route's speed rows with request_date >= watermark, summed column by column
 *
 * @param {object} data { pointer, batch: { [table]: { [routeKey]: stats } },
 *                        speed: [{ route, date, stats }], lookups: { [kind]: [values] } }
 */
function createFakeStore(data = {}) {
  return {
    data,
    async pointer() {
      return data.pointer || null;
    },
    async batchStats(table, route) {
      assertBatchTable(table);
      return ((data.batch || {})[table] || {})[routeKey(route)] || null;
    },
    async speedStats(route, watermark) {
      const sums = {};
      for (const row of data.speed || []) {
        if (routeKey(row.route) !== routeKey(route) || row.date < watermark) continue;
        for (const [column, value] of Object.entries(row.stats)) sums[column] = (sums[column] || 0) + value;
      }
      return sums;
    },
    async lookups(kind) {
      return (data.lookups || {})[kind] || [];
    },
  };
}

module.exports = { createFakeStore, routeKey };
