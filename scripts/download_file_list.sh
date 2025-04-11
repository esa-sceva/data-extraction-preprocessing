#!/bin/bash

# Check if required parameters are provided
if [ $# -lt 3 ]; then
    echo "Usage: $0 <file-list> <source-bucket> <destination-folder>"
    echo "Example: $0 server_1_files.txt my-bucket /local/destination/folder/"
    echo "Example: $0 server_1_files.txt my-bucket s3://dest-bucket/folder/"
    exit 1
fi

# Get parameters
file_list=$1
source_bucket=$2
destination=$3

# Ensure destination folder ends with /
if [[ "$destination" != */ ]]; then
    destination="${destination}/"
fi

# Check if file list exists
if [ ! -f "$file_list" ]; then
    echo "Error: File list '$file_list' not found!"
    exit 1
fi

echo "Copying files from list: $file_list"
echo "Source bucket: $source_bucket"
echo "Destination: $destination"

# Count total files to copy
total_files=$(wc -l < "$file_list")
echo "Total files to copy: $total_files"

# Initialize counter
count=0

# Read each line from the file list and copy the corresponding file
while IFS= read -r file_path; do
    count=$((count + 1))

    # Determine if destination is local or S3
    if [[ "$destination" == s3://* ]]; then
        # S3 to S3 copy
        aws s3 cp "s3://${source_bucket}/${file_path}" "${destination}$(basename "$file_path")"
    else
        # S3 to local copy
        # Create the destination directory if it doesn't exist
        mkdir -p "$destination"
        aws s3 cp "s3://${source_bucket}/${file_path}" "${destination}$(basename "$file_path")"
    fi

    # Display progress
    echo "[$count/$total_files] Copied: $file_path"
done < "$file_list"

echo "Done! All files have been copied to $destination"