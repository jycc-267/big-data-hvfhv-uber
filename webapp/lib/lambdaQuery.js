'use strict';

const { METRICS, TRIP_COUNT } = require('./schema');

/** Adds stats column-by-column; missing columns count as 0. */
function sumStats(statsList) {
  const total = {};
  for (const stats of statsList) {
    for (const [column, value] of Object.entries(stats)) {
      total[column] = (total[column] || 0) + value;
    }
  }
  return total;
}

/**
 * Per-trip averages in display units. Totals and counts from both layers are summed
 * *before* dividing, so each trip carries the same weight whichever layer counted it.
 */
function averages(stats) {
  return METRICS.map(({ name, label, scale }) => {
    const count = stats[`${name}_count`] || 0;
    const value = count > 0 ? Number(((stats[`${name}_total`] || 0) / count / scale).toFixed(2)) : null;
    return { name, label, value, display: value === null ? 'n/a' : value.toFixed(2) };
  });
}

/**
 * Answers route queries from the serving layer:
 *   active batch view row (request dates < watermark) + speed rows (dates >= watermark).
 * The serving pointer is cached briefly; the batch job keeps the previous view and its
 * speed rows alive for one more run, so a slightly stale pointer still answers correctly.
 *
 * @param {object} deps.store see cassandraStore.js (pointer, batchStats, speedStats)
 */
function createRouteQuery({ store, pointerTtlMs = 10000, now = Date.now }) {
  let cached = null;

  async function servingPointer() {
    if (cached && now() - cached.at < pointerTtlMs) return cached.pointer;
    const pointer = await store.pointer();
    if (!pointer || !pointer.batchTable || !pointer.watermark) {
      throw new Error('No batch view has been published yet; run BuildBatchView');
    }
    cached = { pointer, at: now() };
    return pointer;
  }

  return async function query(route) {
    const pointer = await servingPointer();
    const [batch, speed] = await Promise.all([
      store.batchStats(pointer.batchTable, route),
      store.speedStats(route, pointer.watermark),
    ]);
    const total = sumStats([batch || {}, speed]);

    return {
      watermark: pointer.watermark,
      builtAt: pointer.builtAt || null,
      trips: {
        total: total[TRIP_COUNT] || 0,
        batch: (batch && batch[TRIP_COUNT]) || 0,
        speed: speed[TRIP_COUNT] || 0,
      },
      metrics: averages(total),
    };
  };
}

/** Values for the form's drop-downs. */
function createLookups({ store }) {
  return async function lookups() {
    const [carriers, zones, hours] = await Promise.all([
      store.lookups('carrier'),
      store.lookups('zone'),
      store.lookups('hour'),
    ]);
    return { carriers, zones, hours: hours.map(Number).sort((a, b) => a - b) };
  };
}

module.exports = { createRouteQuery, createLookups, sumStats, averages };
