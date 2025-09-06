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
                if not key.endswith("/"):  # exclude folder placeholders
                    file_list.append(os.path.relpath(key, folder_name))
    return set(file_list)


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


def compare_extracted_vs_cleaned(bucket_name, root_path, s3_client, debug=False):
    """
    Compare only subfolders that exist in BOTH data_extracted and data_cleaned.
    Returns dictionary with differences per subfolder.
    """
    extracted_prefix = os.path.join(root_path, "data_extracted/").replace("\\", "/")
    cleaned_prefix = os.path.join(root_path, "data_cleaned/").replace("\\", "/")

    # get list of subfolders in each
    extracted_subs = list_subfolders(bucket_name, extracted_prefix, s3_client)
    cleaned_subs = list_subfolders(bucket_name, cleaned_prefix, s3_client)

    common_subs = extracted_subs & cleaned_subs
    skipped_extracted_only = extracted_subs - cleaned_subs
    skipped_cleaned_only = cleaned_subs - extracted_subs

    if debug:
        print(f"Common subfolders: {len(common_subs)}")
        if skipped_extracted_only:
            print(f"Skipped (only in extracted): {skipped_extracted_only}")
        if skipped_cleaned_only:
            print(f"Skipped (only in cleaned): {skipped_cleaned_only}")

    results = {}
    for sub in tqdm(sorted(common_subs), desc="Comparing folders"):
        extracted_path = extracted_prefix + sub
        cleaned_path = cleaned_prefix + sub

        files_extracted = list_files_in_folder(bucket_name, extracted_path, s3_client)
        files_cleaned = list_files_in_folder(bucket_name, cleaned_path, s3_client)

        missing_in_cleaned = sorted(list(files_extracted - files_cleaned))
        extra_in_cleaned = sorted(list(files_cleaned - files_extracted))

        results[sub] = {
            "total_in_extracted": len(files_extracted),
            "total_in_cleaned": len(files_cleaned),
            "missing_in_cleaned": missing_in_cleaned,
            "extra_in_cleaned": extra_in_cleaned
        }

        if debug:
            print(f"\n{sub}:")
            print(f"  total_in_extracted: {len(files_extracted)}")
            print(f"  total_in_cleaned: {len(files_cleaned)}")
            print(f"  missing_in_cleaned: {len(missing_in_cleaned)}")
            print(f"  extra_in_cleaned: {len(extra_in_cleaned)}")

    return results


if __name__ == "__main__":
    bucket = "esa-satcom-s3"
    root_path = ""  # adjust if your data folder is nested
    output_file = "comparison_results.json"

    s3_client = boto3.client("s3")

    results = compare_extracted_vs_cleaned(bucket, root_path, s3_client, debug=True)

    with open(output_file, "w") as f:
        json.dump(results, f, indent=4)

    print(f"\n✅ Results saved to {output_file}")
