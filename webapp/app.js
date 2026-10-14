'use strict';

require('dotenv').config();

const { loadConfig } = require('./lib/config');
const { createCassandraStore } = require('./lib/cassandraStore');
const { createLookups, createRouteQuery } = require('./lib/lambdaQuery');
const { createApp } = require('./lib/server');

const config = loadConfig();
const store = createCassandraStore(config.cassandra);

const app = createApp({
  routeQuery: createRouteQuery({ store, pointerTtlMs: config.pointerTtlMs }),
  lookups: createLookups({ store }),
});

const server = app.listen(config.port, () => {
  console.log(`HVFHV web app on http://localhost:${config.port} ` +
    `(Cassandra ${config.cassandra.contactPoints.join(',')}/${config.cassandra.keyspace})`);
});

process.on('SIGTERM', () => server.close(() => store.shutdown()));
