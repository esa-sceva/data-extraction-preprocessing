#!/bin/bash

# Paths
SOURCE_DIR="data/mdpi_all"
DEST_DIR="data/mdpi"
FILE_LIST="server_2_files.txt"

# Create destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

while IFS= read -r filepath; do
    filename=$(basename "$filepath")
    src_file="$SOURCE_DIR/$filename"

    if [[ -f "$src_file" ]]; then
        mv "$src_file" "$DEST_DIR/"
    else
        echo "File not found: $src_file"
    fi
done < "$FILE_LIST"