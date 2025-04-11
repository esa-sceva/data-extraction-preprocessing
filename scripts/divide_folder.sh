#!/bin/bash

# Check if required parameters are provided
if [ $# -lt 2 ]; then
    echo "Usage: $0 <s3-folder-path> <number-of-servers>"
    echo "Example: $0 s3://my-bucket/my-folder/ 5"
    exit 1
fi

# Get parameters
s3_folder_path=$1
N=$2

echo "Processing files from: $s3_folder_path"
echo "Splitting into $N server lists"

# List all files in the S3 bucket/folder
aws s3 ls "$s3_folder_path" --recursive | awk '{print $4}' > all_files.txt

# Get total number of files
total_files=$(wc -l < all_files.txt)
echo "Total files found: $total_files"

# Calculate files per server (rounded down)
files_per_server=$((total_files / N))
echo "Each server will process approximately $files_per_server files"

# Split the files into N lists
for ((i=1; i<=N; i++)); do
    start_line=$(( (i-1) * files_per_server + 1 ))

    # For the last chunk, take all remaining files
    if [ $i -eq $N ]; then
        sed -n "${start_line},\$p" all_files.txt > server_${i}_files.txt
    else
        end_line=$((i * files_per_server))
        sed -n "${start_line},${end_line}p" all_files.txt > server_${i}_files.txt
    fi

    echo "Created list for server $i with $(wc -l < server_${i}_files.txt) files"
done

echo "Done! File lists have been created."