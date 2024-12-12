'use strict';

/**
 * The app begins with a lot “require” statements
 * These list node.js packages that are needed by our application, storing them in a constant or variable
 */
const http = require('http');
var assert = require('assert');

/**
 * Express is a web application framework for Node.js
 * It takes care of a lot of the boilerplate for building websites, which will make our app very simple
 * We create an Express object that we will use to represent our web application
 */
const express= require('express');
const app = express(); // our web app!


const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config()

/**
 * Tell Node which hostname and port for the app to listen on
 */
const port = Number(process.argv[2]);

/**
 * Create an HBase client object and set it to log to the console, handy for debugging
 */
const hbase = require('hbase')
const url = new URL(process.argv[3]);
// var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})
var hclient = hbase({
	host: url.hostname,
	port: url.port || 8090, // Use the port from the URL if provided, otherwise default to 8090
	path: url.pathname || '/',
	protocol: 'http', // Remove the colon from the protocol
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});



/**
 * Most of our website is static HTML in the “public” directory.
 * Tell express to statically serve files in the public directory for web requests
 * that are not handled dynamically by Node.js.
 */
app.use(express.static('public'));
app.get('/hvfhv-form.html', function (req, res) {
	let data = {};
	let tablesScanned = 0;

	// Scan carrier table
	hclient.table('jycchien_carrier').scan({ maxVersions: 1 }, (err, carriers) => {
		if (err) {
			console.error('Error scanning carrier table:', err);
			return res.status(500).send('Error fetching data');
		}
		data.carriers = carriers;
		checkComplete();
	});

	// Scan zone table
	hclient.table('jycchien_zone').scan({ maxVersions: 1 }, (err, zones) => {
		if (err) {
			console.error('Error scanning zone table:', err);
			return res.status(500).send('Error fetching data');
		}
		data.zones = zones;
		checkComplete();
	});

	// Scan hours table
	hclient.table('jycchien_hours').scan({ maxVersions: 1 }, (err, hours) => {
		if (err) {
			console.error('Error scanning hours table:', err);
			return res.status(500).send('Error fetching data');
		}
		data.hours = hours;
		checkComplete();
	});

	function checkComplete() {
		tablesScanned++;
		if (tablesScanned === 3) {
			var template = filesystem.readFileSync("submit.mustache").toString();
			var html = mustache.render(template, data);
			res.send(html);
		}
	}
});

app.get('/trip-results.html', function (req, res) {
	const { carrier, pickup_location, dropoff_location, hour } = req.query;
	const rowKey = `${carrier}|${pickup_location}|${dropoff_location}|${hour}`;

	let results = {};
	let tablesQueried = 0;

	function queryTable(tableName) {
		console.log(rowKey);
		hclient.table(tableName).row(rowKey).get(function (err, cells) {
			if (err || !cells || Object.keys(cells).length === 0) {
				if (err && err.code === 404) {
					console.log(`No data found for ${tableName} with key ${rowKey}`);
				} else if (err) {
					console.error(`Error querying ${tableName}:`, err);
				}
				results[tableName] = null;
			} else {
				console.log(cells);
				const tripInfo = rowToMap(cells);
				console.log(tripInfo);
				results[tableName] = {
					congestion_surcharge: tripInfo['stats:congestion_surcharge_count'] !== 0 ?
						tripInfo['stats:total_congestion_surcharge'] / tripInfo['stats:congestion_surcharge_count'] : 0,
					tolls: tripInfo['stats:tolls_count'] !== 0 ?
						tripInfo['stats:total_tolls'] / tripInfo['stats:tolls_count'] : 0,
					revenue: tripInfo['stats:revenue_count'] !== 0 ?
						tripInfo['stats:total_revenue'] / tripInfo['stats:revenue_count'] : 0,
					trip_time: tripInfo['stats:trip_time_count'] !== 0 ?
						(tripInfo['stats:total_trip_time'] / tripInfo['stats:trip_time_count']) / 60 : 0,
					wait_time: tripInfo['stats:wait_time_count'] !== 0 ?
						(tripInfo['stats:total_wait_time'] / tripInfo['stats:wait_time_count']) / 60 : 0
				};
			}

			tablesQueried++;
			if (tablesQueried === 2) {
				calculateAverages();
			}
		});
	}

	function calculateAverages() {
		let averages = {};
		const metrics = ['congestion_surcharge', 'tolls', 'revenue', 'trip_time', 'wait_time'];

		metrics.forEach(metric => {
			let sum = 0;
			let count = 0;
			for (let tableName in results) {
				if (results[tableName] && results[tableName][metric] !== undefined && !isNaN(results[tableName][metric])) {
					sum += results[tableName][metric];
					count++;
				}
			}
			averages[metric] = count > 0 ? parseFloat((sum / count).toFixed(2)) : results['jycchien_hvfhs_route_hourly_summary'][metric] || 'N/A';
		});

		renderResults(averages);
	}

	function renderResults(averages) {
		const data = {
			carrier,
			pickup_location,
			dropoff_location,
			hour,
			averages
		};

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template, data);
		res.send(html);
	}

	queryTable('jycchien_hvfhs_route_hourly_summary');
	queryTable('jycchien_hvfhs_route_hourly_summary_speed');
});


function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		const column = item['column'];
		const value = item['$'];
		stats[column] = Number(Buffer.from(value).readBigInt64BE());
	});
	return stats;
}

app.listen(port);
