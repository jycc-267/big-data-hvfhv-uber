DROP TABLE IF EXISTS jycchien_hvfhs_trip_external;
CREATE EXTERNAL TABLE jycchien_hvfhs_trip_external (
    hvfhs_license_num STRING,
    dispatching_base_num STRING,
    originating_base_num STRING,
    request_datetime TIMESTAMP,
    on_scene_datetime TIMESTAMP,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    trip_miles DOUBLE,
    trip_time BIGINT,
    base_passenger_fare DOUBLE,
    tolls DOUBLE,
    bcf DOUBLE,
    sales_tax DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    tips DOUBLE,
    driver_pay DOUBLE,
    shared_request_flag STRING,
    shared_match_flag STRING,
    access_a_ride_flag STRING,
    wav_request_flag STRING,
    wav_match_flag STRING,
    pickup_borough STRING,
    pickup_zone STRING,
    pickup_service_zone STRING,
    dropoff_borough STRING,
    dropoff_zone STRING,
    dropoff_service_zone STRING,
    hour_in_day BIGINT,
    revenue DOUBLE,
    wait_time BIGINT
)
STORED AS PARQUET
LOCATION "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/jycchien/hvfhs_trip"
TBLPROPERTIES ("parquet.compression" = "SNAPPY");

-- Create managed table to hold the data
DROP TABLE IF EXISTS jycchien_hvfhs_trip;
CREATE TABLE jycchien_hvfhs_trip(
    hvfhs_license_num STRING,
    dispatching_base_num STRING,
    originating_base_num STRING,
    request_datetime TIMESTAMP,
    on_scene_datetime TIMESTAMP,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    trip_miles DOUBLE,
    trip_time BIGINT,
    base_passenger_fare DOUBLE,
    tolls DOUBLE,
    bcf DOUBLE,
    sales_tax DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    tips DOUBLE,
    driver_pay DOUBLE,
    shared_request_flag STRING,
    shared_match_flag STRING,
    access_a_ride_flag STRING,
    wav_request_flag STRING,
    wav_match_flag STRING,
    pickup_borough STRING,
    pickup_zone STRING,
    pickup_service_zone STRING,
    dropoff_borough STRING,
    dropoff_zone STRING,
    dropoff_service_zone STRING,
    hour_in_day BIGINT,
    revenue DOUBLE,
    wait_time BIGINT
)
STORED AS ORC;


-- Copy data from external table to managed table
INSERT OVERWRITE TABLE jycchien_hvfhs_trip
SELECT *
FROM jycchien_hvfhs_trip_external;

DESCRIBE FORMATTED jycchien_hvfhs_trip;