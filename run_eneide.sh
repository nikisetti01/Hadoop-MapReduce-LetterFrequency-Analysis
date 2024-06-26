#!/bin/bash

# Directory containing input files
input_dir="/user/hadoop/Eneide"

# Output directory
output_dir="/user/hadoop/output_eneide"

# Loop through each file in the input directory
for file in $(hadoop fs -ls "$input_dir" | grep '^-' | awk '{print $8}')
do
    # Extract the filename without the path
    filename=$(basename "$file")

    # Extract the language from the filename
    language=$(echo "$filename" | cut -d'_' -f2)

    # Run the MapReduce job
    hadoop jar target/lettercount-1.0-SNAPSHOT.jar it.unipi.hadoop.Start "$file" "$output_dir/output_$language" 1 "InMapper"

    # Print the status of the job
    echo "MapReduce job for $filename completed."
done
