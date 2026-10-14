'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { createLookups, createRouteQuery } = require('../lib/lambdaQuery');
const { assertBatchTable, METRIC_COLUMNS } = require('../lib/schema');
const { toStats } = require('../lib/cassandraStore');
const { createFakeStore, routeKey } = require('./fakeStore');

const route = { carrier: 'Uber', pickup: 'JFK Airport', dropoff: 'Midtown Center', hour: 8 };
const key = routeKey(route);
const pointer = (batchTable, watermark) => ({ batchTable, watermark, builtAt: '2026-10-13T00:00:00Z' });
const metric = (result, name) => result.metrics.find((m) => m.name === name).value;

test('batch and speed are merged by summing totals and counts (weighted), not by averaging averages', async () => {
  const store = createFakeStore({
    pointer: pointer('route_hourly_v1', '20220103'),
    // 10 batch trips averaging $1.00 in tolls, 1 speed trip with $12.00.
    batch: { route_hourly_v1: { [key]: { tolls_total: 1000, tolls_count: 10, trip_count: 10 } } },
    speed: [{ route, date: '20220103', stats: { tolls_total: 1200, tolls_count: 1, trip_count: 1 } }],
  });
  const result = await createRouteQuery({ store })(route);

  assert.equal(metric(result, 'tolls'), 2.0); // (1000 + 1200) / 11 cents; the original app reported 6.50
  assert.deepEqual(result.trips, { total: 11, batch: 10, speed: 1 });
  assert.equal(result.watermark, '20220103');
});

test('speed rows dated before the watermark are excluded because the batch view already counts them', async () => {
  const store = createFakeStore({
    pointer: pointer('route_hourly_v2', '20220103'),
    batch: { route_hourly_v2: { [key]: { trip_time_total: 600, trip_time_count: 1, trip_count: 1 } } },
    speed: [
      { route, date: '20220102', stats: { trip_time_total: 99999, trip_time_count: 9, trip_count: 9 } },
      { route, date: '20220103', stats: { trip_time_total: 1200, trip_time_count: 1, trip_count: 1 } },
      // Two micro-batches on the same day are separate rows that both count.
      { route, date: '20220104', stats: { trip_time_total: 900, trip_time_count: 1, trip_count: 1 } },
      { route, date: '20220104', stats: { trip_time_total: 900, trip_time_count: 1, trip_count: 1 } },
      { route: { ...route, hour: 18 }, date: '20220103', stats: { trip_time_total: 1, trip_time_count: 1, trip_count: 1 } },
    ],
  });
  const result = await createRouteQuery({ store })(route);

  assert.equal(metric(result, 'trip_time'), 15); // (600 + 1200 + 900 + 900) s / 4 trips / 60
  assert.equal(result.trips.total, 4);
});

test('a route with only speed data, or with no data at all, is answered without errors', async () => {
  const store = createFakeStore({
    pointer: pointer('route_hourly_v1', '20220103'),
    speed: [{ route, date: '20220103', stats: { wait_time_total: 300, wait_time_count: 1, trip_count: 1 } }],
  });
  const query = createRouteQuery({ store });

  const speedOnly = await query(route);
  assert.equal(metric(speedOnly, 'wait_time'), 5);
  assert.equal(metric(speedOnly, 'revenue'), null);
  assert.equal(speedOnly.metrics.find((m) => m.name === 'revenue').display, 'n/a');

  const nothing = await query({ ...route, hour: 3 });
  assert.equal(nothing.trips.total, 0);
});

test('the serving pointer is cached briefly and then re-read, so a new batch view is picked up', async () => {
  let clock = 0;
  const store = createFakeStore({
    pointer: pointer('route_hourly_v1', '20220103'),
    batch: {
      route_hourly_v1: { [key]: { trip_count: 1 } },
      route_hourly_v2: { [key]: { trip_count: 2 } },
    },
  });
  const query = createRouteQuery({ store, pointerTtlMs: 1000, now: () => clock });

  assert.equal((await query(route)).trips.batch, 1);
  store.data.pointer = pointer('route_hourly_v2', '20220104');
  clock = 500;
  assert.equal((await query(route)).trips.batch, 1, 'still within the TTL');
  clock = 1500;
  const refreshed = await query(route);
  assert.equal(refreshed.trips.batch, 2);
  assert.equal(refreshed.watermark, '20220104');
});

test('a missing serving pointer is reported as an error', async () => {
  const query = createRouteQuery({ store: createFakeStore({}) });
  await assert.rejects(query(route), /No batch view has been published/);
});

test('only well-formed batch view names are ever put into a CQL statement', () => {
  assert.equal(assertBatchTable('route_hourly_v1791930729532'), 'route_hourly_v1791930729532');
  for (const bad of ['speed', 'route_hourly_v1; DROP TABLE speed', 'route_hourly_v', 'x.route_hourly_v1']) {
    assert.throws(() => assertBatchTable(bad), /unexpected table name/, bad);
  }
});

test('Cassandra bigints (driver Long objects) become plain numbers; missing rows stay null', () => {
  const asLong = (n) => ({ toString: () => String(n) });
  const row = Object.fromEntries(METRIC_COLUMNS.map((c, i) => [c, asLong(i * 1000)]));
  const stats = toStats(row);
  assert.equal(stats.trip_count, (METRIC_COLUMNS.length - 1) * 1000);
  assert.equal(toStats(null), null);
});

test('lookups return drop-down values with hours in numeric order', async () => {
  const store = createFakeStore({
    lookups: { carrier: ['Lyft', 'Uber'], zone: ['JFK Airport'], hour: ['0', '1', '10', '2', '23'] },
  });
  assert.deepEqual(await createLookups({ store })(), {
    carriers: ['Lyft', 'Uber'],
    zones: ['JFK Airport'],
    hours: [0, 1, 2, 10, 23],
  });
});
