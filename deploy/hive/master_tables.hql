-- The batch layer's master dataset (the ground truth). Hive owns these definitions;
-- Spark jobs read and write the tables through the Hive Metastore.
--
--   scripts/hive-init.sh          (runs this file through beeline in the hiveserver2 container)
--
-- Every table is EXTERNAL: Hive 4 makes managed tables transactional (ACID) by default,
-- and Spark can't read ACID tables. Locations are absolute paths that every container
-- mounts at the same place (/data). Column order must match TripSchema.Canonical.

CREATE DATABASE IF NOT EXISTS hvfhv LOCATION 'file:///data/warehouse/hvfhv.db';

-- Historical bulk download; IngestMasterDataset replaces it as a whole.
CREATE EXTERNAL TABLE IF NOT EXISTS hvfhv.trips_bulk (
    hvfhs_license_num STRING,
    dispatching_base_num STRING,
    originating_base_num STRING,
    request_datetime TIMESTAMP,
    on_scene_datetime TIMESTAMP,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    pulocationid INT,
    dolocationid INT,
    trip_miles DOUBLE,
    trip_time BIGINT,
    base_passenger_fare DECIMAL(10,2),
    tolls DECIMAL(10,2),
    bcf DECIMAL(10,2),
    sales_tax DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee DECIMAL(10,2),
    tips DECIMAL(10,2),
    driver_pay DECIMAL(10,2),
    shared_request_flag STRING,
    shared_match_flag STRING,
    access_a_ride_flag STRING,
    wav_request_flag STRING,
    wav_match_flag STRING
)
COMMENT 'HVFHV trips from the historical bulk download'
PARTITIONED BY (request_date STRING)
STORED AS ORC
LOCATION 'file:///data/warehouse/hvfhv.db/trips_bulk';

-- Trips that arrived through the speed layer; each BuildBatchView run absorbs the
-- committed landing-zone files into it (de-duplicated, only touched dates rewritten).
CREATE EXTERNAL TABLE IF NOT EXISTS hvfhv.trips_stream
LIKE hvfhv.trips_bulk
LOCATION 'file:///data/warehouse/hvfhv.db/trips_stream';

CREATE EXTERNAL TABLE IF NOT EXISTS hvfhv.zones (
    locationid INT,
    borough STRING,
    zone STRING,
    service_zone STRING
)
COMMENT 'NYC taxi zone lookup'
STORED AS ORC
LOCATION 'file:///data/warehouse/hvfhv.db/zones';

-- Every trip the batch layer knows about, for HiveQL users. The split date keeps the two
-- tables disjoint and absorption de-duplicates replays, so this is the full ground truth.
CREATE VIEW IF NOT EXISTS hvfhv.trips AS
SELECT * FROM hvfhv.trips_bulk
UNION ALL
SELECT * FROM hvfhv.trips_stream;
