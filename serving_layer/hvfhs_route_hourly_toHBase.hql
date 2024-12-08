DROP TABLE IF EXISTS jycchien_hvfhs_route_hourly;
CREATE TABLE jycchien_hvfhs_route_hourly (
  hvfhs_license_num STRING,
  pickup_zone STRING,
  dropoff_zone STRING,
  hour_in_day BIGINT,
  congestion_surcharge_count BIGINT,
  total_congestion_surcharge DOUBLE,
  tolls_count BIGINT,
  total_tolls DOUBLE,
  revenue_count BIGINT,
  total_revenue DOUBLE,
  trip_time_count BIGINT,
  total_trip_time DOUBLE,
  wait_time_count BIGINT,
  total_wait_time DOUBLE
);

INSERT OVERWRITE TABLE jycchien_hvfhs_route_hourly
SELECT
  hvfhs_license_num,
  pickup_zone,
  dropoff_zone,
  hour_in_day,
  COUNT(IF(congestion_surcharge IS NOT NULL, 1, 0)) AS congestion_surcharge_count,
  SUM(congestion_surcharge) AS total_congestion_surcharge,
  COUNT(IF(tolls IS NOT NULL, 1, 0)) AS tolls_count,
  SUM(tolls) AS total_tolls,
  COUNT(IF(revenue IS NOT NULL, 1, 0)) AS revenue_count,
  SUM(revenue) AS total_revenue,
  COUNT(IF(trip_time IS NOT NULL, 1, 0)) AS trip_time_count,
  SUM(trip_time) AS total_trip_time,
  COUNT(IF(wait_time IS NOT NULL, 1, 0)) AS wait_time_count,
  SUM(wait_time) AS total_wait_time
FROM
  jycchien_hvfhs_trip
WHERE
  hvfhs_license_num IS NOT NULL AND
  pickup_zone IS NOT NULL AND
  dropoff_zone IS NOT NULL AND
  hour_in_day IS NOT NULL
GROUP BY
  hvfhs_license_num,
  pickup_zone,
  dropoff_zone,
  hour_in_day;

DROP TABLE IF EXISTS jycchien_hvfhs_route_hourly_summary;
CREATE EXTERNAL TABLE jycchien_hvfhs_route_hourly_summary (
  hourly_route STRING,
  congestion_surcharge_count BIGINT,
  total_congestion_surcharge DOUBLE,
  tolls_count BIGINT,
  total_tolls DOUBLE,
  revenue_count BIGINT,
  total_revenue DOUBLE,
  trip_time_count BIGINT,
  total_trip_time DOUBLE,
  wait_time_count BIGINT,
  total_wait_time DOUBLE
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
-- HBase has a "counter" value type that is a 64 bit integer, more suitable for arithmetic
-- Put a "b" in the hbase.columns.mapping to indicate that that value is binary
-- The Hive schema should make that column a long, which is how Hive represents 64-bit integers
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,
                                                stats:congestion_surcharge_count#b,
                                                stats:total_congestion_surcharge#b,
                                                stats:tolls_count#b,
                                                stats:total_tolls#b,
                                                stats:revenue_count#b,
                                                stats:total_revenue#b,
                                                stats:trip_time_count#b,
                                                stats:total_trip_time#b,
                                                stats:wait_time_count#b,
                                                stats:total_wait_time#b');

INSERT OVERWRITE TABLE jycchien_hvfhs_route_hourly_summary
SELECT
concat_ws('|', hvfhs_license_num, pickup_zone, dropoff_zone, cast(hour_in_day as string)) as hourly_route,
    congestion_surcharge_count,
    total_congestion_surcharge,
    tolls_count,
    total_tolls,
    revenue_count,
    total_revenue,
    trip_time_count,
    total_trip_time,
    wait_time_count,
    total_wait_time
FROM jycchien_hvfhs_route_hourly;