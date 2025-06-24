import os
import boto3
from pathlib import Path

def upload_folder_to_s3(local_folder, bucket_name, s3_prefix):
    s3 = boto3.client('s3')
    local_folder = Path(local_folder)

    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_path = Path(root) / file
            relative_path = local_path.relative_to(local_folder)
            s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/") # probably dont need tthe backslash
            print(f"Uploading {local_path} to s3://{bucket_name}/{s3_key}")
            s3.upload_file(str(local_path), bucket_name, s3_key)

bucket_name = "llm4eo-s3"
local_logs_folder = "logs"

upload_folder_to_s3(local_logs_folder, bucket_name, "raw_data_dedup_cleaned_v2")