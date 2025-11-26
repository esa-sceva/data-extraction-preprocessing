import boto3
import os
import json
from tqdm import tqdm


def list_files_in_folder(bucket_name, folder_name, s3_client):
    """
    List all files in a given S3 folder.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    operation_parameters = {
        "Bucket": bucket_name,
        "Prefix": folder_name if folder_name.endswith("/") else folder_name + "/"
    }

    file_list = []
    for page in paginator.paginate(**operation_parameters):
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                if not key.endswith("/"):
                    file_list.append(os.path.relpath(key, folder_name))
    return set(file_list)


def count_characters_in_file(bucket_name, key, s3_client):
    """
    Count characters in a single S3 file by streaming line by line.
    """
    total_chars = 0
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    for line in obj['Body'].iter_lines():
        total_chars += len(line.decode('utf-8', errors='ignore'))
    return total_chars


def count_characters_in_folder(bucket_name, folder_name, s3_client):
    """
    Count total characters in all files under the S3 folder.
    Returns total characters and number of files.
    """
    total_chars = 0
    file_list = list_files_in_folder(bucket_name, folder_name, s3_client)
    for file_name in tqdm(file_list, desc=f"Counting chars in {folder_name}", leave=False):
        s3_key = os.path.join(folder_name, file_name).replace("\\", "/")
        total_chars += count_characters_in_file(bucket_name, s3_key, s3_client)
    return total_chars, len(file_list)


def list_subfolders(bucket_name, prefix, s3_client):
    """
    List immediate subfolders under a given prefix.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    subfolders = set()
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        if "CommonPrefixes" in page:
            for p in page["CommonPrefixes"]:
                subfolder = p["Prefix"].replace(prefix, "")
                if subfolder:
                    subfolders.add(subfolder)
    return subfolders


if __name__ == "__main__":
    bucket = ""
    root_path = ""  # adjust if your data folder is nested
    cleaned_prefix = os.path.join(root_path, "data/").replace("\\", "/")
    output_file = "chars_and_files_in_data_pii_removal.json"

    s3_client = boto3.client("s3")

    # get all subfolders in data_cleaned
    subfolders = list_subfolders(bucket, cleaned_prefix, s3_client)

    results = {}
    for sub in tqdm(sorted(subfolders), desc="Processing subfolders"):
        folder_path = cleaned_prefix + sub
        total_chars, num_files = count_characters_in_folder(bucket, folder_path, s3_client)
        results[sub] = {
            "chars_in_cleaned": total_chars,
            "num_files": num_files
        }

    # save results to JSON
    with open(output_file, "w") as f:
        json.dump(results, f, indent=4)

    print(f"\nCharacter counts and file numbers saved to {output_file}")
