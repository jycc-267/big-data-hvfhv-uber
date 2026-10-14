'use strict';

const fs = require('fs');
const path = require('path');
const express = require('express');
const mustache = require('mustache');
const { parseRouteQuery } = require('./validate');

const VIEWS_DIR = path.join(__dirname, '..', 'views');
const PUBLIC_DIR = path.join(__dirname, '..', 'public');

function loadViews() {
  const read = (name) => fs.readFileSync(path.join(VIEWS_DIR, name), 'utf8');
  return { submit: read('submit.mustache'), result: read('result.mustache') };
}

/** Express 4 does not forward rejected promises to the error handler on its own. */
const asyncRoute = (handler) => (req, res, next) => Promise.resolve(handler(req, res)).catch(next);

/**
 * @param {object} deps
 * @param {(route: object) => Promise<object>} deps.routeQuery see lambdaQuery.createRouteQuery
 * @param {() => Promise<object>} deps.lookups see lambdaQuery.createLookups
 */
function createApp({ routeQuery, lookups, views = loadViews(), logger = console }) {
  const app = express();
  app.disable('x-powered-by');
  app.use(express.static(PUBLIC_DIR));

  app.get('/', (req, res) => res.redirect('/hvfhv-form.html'));

  app.get('/hvfhv-form.html', asyncRoute(async (req, res) => {
    res.send(mustache.render(views.submit, await lookups()));
  }));

  app.get('/trip-results.html', asyncRoute(async (req, res) => {
    const { route, errors } = parseRouteQuery(req.query);
    if (errors) {
      return res.status(400).send(mustache.render(views.result, { errors }));
    }
    const result = await routeQuery(route);
    return res.send(mustache.render(views.result, { route, ...result, found: result.trips.total > 0 }));
  }));

  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, next) => {
    logger.error(`${req.method} ${req.originalUrl} failed:`, err);
    res.status(500).send('Error fetching data');
  });

  return app;
}

module.exports = { createApp };
