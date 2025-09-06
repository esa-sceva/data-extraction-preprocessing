import boto3
import json
import os
from tqdm import tqdm
from botocore.exceptions import ClientError

# --- CONFIG ---
bucket_name = "esa-satcom-s3"
local_root_extracted = "/data_cleaned"
json_file = "./comparison_results.json"
s3_root_cleaned = "data_pii_removal"  # S3 folder prefix
s3_root_extracted = "data_cleaned"  # S3 folder prefix where missing files are currently

s3 = boto3.client("s3")

# Load JSON
with open(json_file, "r") as f:
    comparison = json.load(f)

for folder, info in comparison.items():
    missing_files = info.get("missing_in_cleaned", [])
    if not missing_files:
        continue

    print(f"\nCopying {len(missing_files)} missing files in {folder}...")

    for filename in tqdm(missing_files, desc=folder, unit="file"):
        source_key = f"{s3_root_extracted}/{folder}{filename}"
        dest_key = f"{s3_root_cleaned}/{folder}{filename}"

        # Copy object within S3
        copy_source = {"Bucket": bucket_name, "Key": source_key}
        try:
            s3.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=dest_key)
        except s3.exceptions.NoSuchKey:
            print(f"WARNING: {source_key} does not exist on S3, skipping...")
