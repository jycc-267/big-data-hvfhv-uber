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
var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port ?? 'http' ? 80 : 443, // http or https defaults
	protocol: url.protocol.slice(0, -1), // Don't want the colon
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});

// static webpage
// change the external table stored in Hbase here!
hclient.table('jycchien_hw42_weather_delays_by_origin_hbase').row('ORD').get((error, value) => {
	console.info(value) // showing the cell value in binary
})

function counterToNumber(c) {
	return Number(Buffer.from(c, 'latin1').readBigInt64BE());
}
function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}

/*
hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})

*/

/**
 * Most of our website is static HTML in the “public” directory.
 * Tell express to statically serve files in the public directory for web requests
 * that are not handled dynamically by Node.js.
 */
app.use(express.static('public'));

/**
 * The frame delays.html shows the weather delays
 * depends on what route was entered, need to generate it dynamically.
 * This says that any requests for delays.html should call this function
 * with a web request and response as arguments.
 */
app.get('/delays.html', function (req, res) {
    // const route=req.query['origin'] + req.query['dest'];
	const origin = req.query['origin']; // row key
    console.log(origin);
	// change the external table stored in Hbase here!
	// hclient.table().scan(filter...) is also possible, check the documentation.
	hclient.table('jycchien_hw42_weather_delays_by_origin_hbase').row(origin).get(function (err, cells) {
		const weatherInfo = rowToMap(cells);
		console.log(weatherInfo) // showing the queried cell values in number for the dynamic webpage
		function weather_delay(weather) {
			var flights = weatherInfo["delay:" + weather + "_flights"];
			var delays = weatherInfo["delay:" + weather + "_delays"];
			if(flights == 0 || flights == null)
				return " - ";
			return (delays/flights).toFixed(1); /* One decimal place */
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			origin : origin,
			//dest : req.query['dest'],
			clear_dly : weather_delay("clear"),
			fog_dly : weather_delay("fog"),
			rain_dly : weather_delay("rain"),
			snow_dly : weather_delay("snow"),
			hail_dly : weather_delay("hail"),
			thunder_dly : weather_delay("thunder"),
			tornado_dly : weather_delay("tornado")
		});
		res.send(html);
	});
});


app.listen(port);
