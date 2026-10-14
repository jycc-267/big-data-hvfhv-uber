'use strict';

/**
 * Settings come from the environment. The legacy positional form `node app.js <port>`
 * still works.
 */
function loadConfig(env = process.env, argv = process.argv.slice(2)) {
  const port = Number(env.PORT || argv[0] || 3000);
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`Invalid port: ${env.PORT || argv[0]}`);
  }
  return {
    port,
    cassandra: {
      contactPoints: (env.CASSANDRA_CONTACT_POINTS || 'localhost').split(',').map((s) => s.trim()),
      localDataCenter: env.CASSANDRA_LOCAL_DC || 'datacenter1',
      keyspace: env.CASSANDRA_KEYSPACE || 'hvfhv',
    },
    pointerTtlMs: Number(env.SERVING_POINTER_TTL_MS || 10000),
  };
}

module.exports = { loadConfig };
