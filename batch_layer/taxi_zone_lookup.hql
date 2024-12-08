-- Create external table to hold the csv data
DROP TABLE IF EXISTS jycchien_zone_lookup_external;
CREATE EXTERNAL TABLE jycchien_zone_lookup_external (
    LocationID STRING,
    Borough STRING,
    Zone STRING,
    service_zone STRING
)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\,"
)
STORED AS TEXTFILE
LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/jycchien/lookup'
TBLPROPERTIES("skip.header.line.count" = "1");

-- Check if external table is successfully established
show tblproperties jycchien_zone_lookup_external;
describe formatted jycchien_zone_lookup_external;

-- Create managed table to hold the data
DROP TABLE IF EXISTS jycchien_zone_lookup;
CREATE TABLE jycchien_zone_lookup(
    LocationID STRING,
    Borough STRING,
    Zone STRING,
    service_zone STRING
)
STORED AS ORC;

-- Copy data from external table to managed table
INSERT OVERWRITE TABLE jycchien_zone_lookup
SELECT *
FROM jycchien_zone_lookup_external;

-- Check if the overwrite operation is successful
show tblproperties jycchien_zone_lookup;
describe formatted jycchien_zone_lookup;