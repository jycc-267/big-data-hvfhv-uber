'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { createApp } = require('../lib/server');
const { parseRouteQuery } = require('../lib/validate');

async function withServer(deps, fn) {
  const server = createApp({ logger: { error() {} }, ...deps }).listen(0);
  await new Promise((resolve) => server.once('listening', resolve));
  try {
    await fn(`http://127.0.0.1:${server.address().port}`);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

const result = {
  watermark: '20220103',
  builtAt: null,
  trips: { total: 3, batch: 2, speed: 1 },
  metrics: [{ name: 'tolls', label: 'Tolls ($)', value: 0, display: '0.00' }],
};

test('parseRouteQuery accepts a valid route and rejects bad input', () => {
  assert.deepEqual(
    parseRouteQuery({ carrier: 'Uber', pickup_location: ' JFK Airport ', dropoff_location: 'Midtown Center', hour: '8' }),
    { route: { carrier: 'Uber', pickup: 'JFK Airport', dropoff: 'Midtown Center', hour: 8 } });

  const { errors } = parseRouteQuery({ carrier: 'x'.repeat(101), pickup_location: '', dropoff_location: ['a', 'b'], hour: '24' });
  assert.equal(errors.length, 4);
  assert.ok(parseRouteQuery({ carrier: 'Uber', pickup_location: 'a', dropoff_location: 'b', hour: '7.5' }).errors);
});

test('the results page renders merged metrics, including zero values', async () => {
  let asked;
  const routeQuery = async (route) => { asked = route; return result; };
  await withServer({ routeQuery, lookups: async () => ({}) }, async (base) => {
    const res = await fetch(`${base}/trip-results.html?carrier=Uber&pickup_location=JFK%20Airport&dropoff_location=Midtown%20Center&hour=8`);
    const html = await res.text();
    assert.equal(res.status, 200);
    assert.deepEqual(asked, { carrier: 'Uber', pickup: 'JFK Airport', dropoff: 'Midtown Center', hour: 8 });
    assert.match(html, /<td>0\.00<\/td>/);
    assert.match(html, /3 trips: 2 from the batch view \(requests before 20220103\)/);
  });
});

test('invalid input is a 400 and never reaches HBase', async () => {
  const routeQuery = async () => assert.fail('must not query');
  await withServer({ routeQuery, lookups: async () => ({}) }, async (base) => {
    const res = await fetch(`${base}/trip-results.html?carrier=Uber&hour=99`);
    assert.equal(res.status, 400);
    assert.match(await res.text(), /Hour of day must be/);
  });
});

test('backend failures become one 500 response instead of crashing or double-sending', async () => {
  const lookups = async () => { throw new Error('HBase REST unreachable'); };
  await withServer({ routeQuery: async () => result, lookups }, async (base) => {
    const res = await fetch(`${base}/hvfhv-form.html`);
    assert.equal(res.status, 500);
    assert.equal(await res.text(), 'Error fetching data');
  });
});

test('the form lists every lookup value', async () => {
  const lookups = async () => ({ carriers: ['Lyft', 'Uber'], zones: ['JFK Airport'], hours: [0, 1] });
  await withServer({ routeQuery: async () => result, lookups }, async (base) => {
    const html = await (await fetch(`${base}/hvfhv-form.html`)).text();
    for (const value of ['Lyft', 'Uber', 'JFK Airport', '0', '1']) {
      assert.ok(html.includes(`<option value="${value}">`), value);
    }
  });
});
