#!/bin/bash

# Define the URL and HDFS path
url="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
hdfs_path="/jycchien/lookup"

# Create the HDFS directory if it doesn't exist
hdfs dfs -mkdir -p $hdfs_path

# Download the file and pipe it directly into HDFS
curl $url | hdfs dfs -put - $hdfs_path/taxi_zone_lookup.csv