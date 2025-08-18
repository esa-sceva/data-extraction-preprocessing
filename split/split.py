import boto3
import math
import random
import argparse
from typing import List
import os

def split_into_numbered_folders(
    bucket_name: str,
    source_prefix: str,
    fraction: float,
    region_name: str = 'us-east-1'
):
    """
    Split files into numbered subfolders within a _splitted folder
    
    Args:
        bucket_name: S3 bucket name
        source_prefix: Source folder path (must end with '/')
        fraction: Fraction for each subfolder (e.g., 0.25 creates 4 subfolders)
        region_name: AWS region name
    """
    s3 = boto3.client('s3', region_name=region_name)
    
    # Validate fraction and calculate number of subfolders
    if fraction <= 0 or fraction >= 1:
        raise ValueError("Fraction must be between 0 and 1")
    
    num_subfolders = round(1 / fraction)
    if not math.isclose(1 / fraction, num_subfolders, rel_tol=1e-9):
        raise ValueError("Fraction must be 1/n where n is an integer (e.g., 0.25, 0.2, etc.)")
    
    # Extract the main folder name
    base_name = os.path.basename(os.path.dirname(source_prefix.rstrip('/') + '/'))
    if not base_name:
        base_name = "split"
    
    # Create the main splitted folder path
    splitted_root = source_prefix.rstrip('/') + "_splitted/"
    
    # List all objects in the source folder
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=source_prefix)
    
    # Get all files (excluding subfolders)
    all_files = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if not obj['Key'].endswith('/'):
                    all_files.append(obj['Key'])
    
    if not all_files:
        print(f"No files found in {source_prefix}")
        return
    
    random.shuffle(all_files)  # Shuffle for random distribution
    
    # Calculate files per subfolder
    files_per_subfolder = len(all_files) // num_subfolders
    remainder = len(all_files) % num_subfolders
    
    print(f"Splitting {len(all_files)} files into {num_subfolders} numbered folders "
          f"in {splitted_root} with ~{files_per_subfolder} files each")
    
    # Create numbered subfolders
    for i in range(num_subfolders):
        # Calculate file range for this subfolder
        start = i * files_per_subfolder
        end = (i + 1) * files_per_subfolder
        if i < remainder:
            end += 1
        
        subfolder_files = all_files[start:end]
        subfolder_name = f"{splitted_root}{base_name}_{i+1}/"
        
        print(f"Creating {subfolder_name} with {len(subfolder_files)} files")
        
        # Copy files to new numbered folder
        for file_key in subfolder_files:
            # Remove source prefix and add to new structure
            relative_path = file_key[len(source_prefix):]
            new_key = subfolder_name + relative_path
            
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': file_key},
                Key=new_key
            )
    
    print(f"Successfully created {num_subfolders} numbered folders in {splitted_root}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Split S3 files into numbered folders within a _splitted directory'
    )
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--prefix', help='Source folder prefix (must end with "/")')
    parser.add_argument('--fraction', type=float, 
                       help='Fraction for each subfolder (e.g., 0.25 for 4 subfolders)')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    
    args = parser.parse_args()
    
    if not args.prefix.endswith('/'):
        args.prefix += '/'
    
    try:
        split_into_numbered_folders(
            bucket_name=args.bucket,
            source_prefix=args.prefix,
            fraction=args.fraction,
            region_name=args.region
        )
    except ValueError as e:
        print(f"Error: {e}")
        print("Fraction must be in form 1/n where n is integer (e.g., 0.25, 0.2, 0.1)")

# Example usage:
# python s3_splitter.py my-bucket data/ 0.25
# Creates: data_splitted/data_1/, data_splitted/data_2/, etc.