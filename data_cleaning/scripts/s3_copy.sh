#!/bin/bash

BUCKET = "llm4eo-s3"
DEST_DIR = "./data"
FILE_LIST = "sampled_5k_original_formats.txt"
PREFIX_TO_STRIP = "raw_data_unduplicated/"

mkdir -p "$DEST_DIR"

while IFS= read -r key; do
  [ -z "$key" ] && continue

  # Strip the prefix from the key
  relative_path="${key#${PREFIX_TO_STRIP}}"

  # Create local directory structure without the prefix
  mkdir -p "$DEST_DIR/$(dirname "$relative_path")"

  # Download the file into the new path
  aws s3 cp "s3://$BUCKET/$key" "$DEST_DIR/$relative_path"
done < "$FILE_LIST"