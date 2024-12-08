-- Create external table to hold the csv data
DROP TABLE IF EXISTS jycchien_fhv_tripdata_external;
CREATE EXTERNAL TABLE jycchien_fhv_tripdata_external (
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
    wav_match_flag STRING
)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\,",
    "timestamp.formats" = "MM/dd/yyyy hh:mm:ss a"
)
STORED AS TEXTFILE
LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/jycchien/fhvdata'
TBLPROPERTIES("skip.header.line.count" = "1");

-- Check if external table is successfully established
SHOW TBLPROPERTIES jycchien_fhv_tripdata_external;
DESCRIBE FORMATTED jycchien_fhv_tripdata_external;

-- Create managed table to hold the data
DROP TABLE IF EXISTS jycchien_fhv_tripdata;
CREATE TABLE jycchien_fhv_tripdata(
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
    wav_match_flag STRING
)
STORED AS ORC;

-- Copy data from external table to managed table
INSERT OVERWRITE TABLE jycchien_fhv_tripdata
SELECT *
FROM jycchien_fhv_tripdata_external;

-- Check if the overwrite operation is successful
SHOW TBLPROPERTIES jycchien_fhv_tripdata;
DESCRIBE FORMATTED jycchien_fhv_tripdata;