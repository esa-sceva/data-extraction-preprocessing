import os
import boto3

BUCKET = "xxx"
DEST_DIR = "./data"
FILE_LIST = "xxx.txt"
PREFIX_TO_STRIP = "raw_data_unduplicated/"

s3 = boto3.client("s3")

os.makedirs(DEST_DIR, exist_ok=True)

with open(FILE_LIST, "r") as f:
    for line in f:
        key = line.strip()
        if not key or not key.startswith(PREFIX_TO_STRIP):
            continue 

        relative_path = key[len(PREFIX_TO_STRIP):]

        dest_path = os.path.join(DEST_DIR, relative_path)

        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        try:
            print(f"Downloading s3://{BUCKET}/{key} to {dest_path}")
            s3.download_file(BUCKET, key, dest_path)
        except Exception as e:
            print(f"Failed to download {key}: {e}")
