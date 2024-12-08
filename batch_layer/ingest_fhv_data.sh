#!/bin/bash

# Download 2022 High Volume FHV Trip Records from NYC OpenData
base_url="https://data.cityofnewyork.us/resource/g6pj-fsah.csv"
limit=500000 # Number of records to download per request
offset=0 # Starting point for each download chunk
max_offset=20000000 # Maximum offset to prevent infinite loops
hdfs_path="/jycchien/fhvdata"

# Create the HDFS directory if it doesn't exist
hdfs dfs -mkdir -p $hdfs_path

# Loop to download data in chunks
while [ $offset -lt $max_offset ]; do
    output_file="fhv_data_${offset}.csv"
    
    # Download data to "output_file" using curl with limit and offset
    curl -o $output_file "${base_url}?\$limit=${limit}&\$offset=${offset}&\$order=request_datetime"
    
    # Check if the downloaded file is empty or contains only a header
    # -l tells wc to count only the number of lines in output_file
    if [ ! -s $output_file ] || [ $(wc -l < $output_file) -le 1 ]; then
        rm $output_file
        break
    fi
    
    # Put the local output_file into HDFS
    hdfs dfs -put -f $output_file $hdfs_path/
    
    # Remove the local file
    rm $output_file
    
    # Increment offset for the next chunk
    offset=$((offset + limit))
    
    echo "Downloaded and stored chunk with offset $offset"
done

echo "Download complete. Data stored in HDFS at $hdfs_path"