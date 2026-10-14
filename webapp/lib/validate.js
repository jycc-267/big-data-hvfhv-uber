'use strict';

const TEXT_FIELDS = [
  ['carrier', 'carrier', 'Carrier'],
  ['pickup_location', 'pickup', 'Pickup location'],
  ['dropoff_location', 'dropoff', 'Dropoff location'],
];

/** Request query -> { route } or { errors }. Values become bound CQL parameters. */
function parseRouteQuery(query) {
  const errors = [];
  const route = {};

  for (const [param, field, label] of TEXT_FIELDS) {
    const value = typeof query[param] === 'string' ? query[param].trim() : '';
    if (!value) errors.push(`${label} is required.`);
    else if (value.length > 100) errors.push(`${label} is not valid.`);
    else route[field] = value;
  }

  const hour = typeof query.hour === 'string' ? query.hour.trim() : '';
  if (!/^\d{1,2}$/.test(hour) || Number(hour) > 23) errors.push('Hour of day must be a whole number from 0 to 23.');
  else route.hour = Number(hour);

  return errors.length ? { errors } : { route };
}

module.exports = { parseRouteQuery };
