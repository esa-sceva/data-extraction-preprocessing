"""
This code uses multithreading to extract text from PDF only using nougat. This setup assumes 4 fastapi servers and requests distributed
in round robin fashion.
"""
import os
import boto3
from typing import Final
from pathlib import Path
from tqdm.auto import tqdm
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import tempfile
import re
import requests

class DataExtractionS3Pipeline:
    def __init__(self, bucket = "esa-satcom-s3", prefix = "MS2/sample/pdfs/", save_to_local=False, max_workers=4, destination_bucket="raw_data_dedup_extractions"):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"  # ensure trailing slash
        self.save_to_local = save_to_local
        self.destination_bucket = destination_bucket  
        self.max_workers = max_workers

        self.s3_client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )

        self.pdf_servers = [
            "http://127.0.0.0:8002/predict/"
            #"http://127.0.0.1:8003/predict/",
            #"http://127.0.0.1:8004/predict/",
            #"http://127.0.0.1:8005/predict/"
        ]

        print(f"Reading PDFs from s3://{self.bucket}/{self.prefix}")



    def _setup_directories(self, sub_folder):
        os.makedirs(f"{self.destination_bucket}/{sub_folder}", exist_ok=True)

    def process_files(self):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)

        pdf_keys = []
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.lower().endswith('.pdf'):
                    pdf_keys.append(key)

        print(f"Found {len(pdf_keys)} PDF files in S3")

        tasks = []
        for i, key in enumerate(pdf_keys):
            server = self.pdf_servers[i % len(self.pdf_servers)]
            tasks.append((key, server))

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.process_pdf_from_s3, *task) for task in tasks]
            for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing S3 PDFs"):
                pass


    def process_pdf_from_s3(self, key, endpoint):
        # print(f"[DEBUG] Processing key: {key}, type: {type(key)}")

        filename = os.path.basename(key)
        local_path = os.path.join(tempfile.gettempdir(), filename)

        try:
            # print(f"[DEBUG] Creating Path from key: {key}")
            provider_path = Path(key).parent
            print(f"[DEBUG] Provider path created: {provider_path}, type: {type(provider_path)}")
            provider_str = str(provider_path).replace('\\', '/')  # Use forward slashes
            # print(f"[DEBUG] Provider string: {provider_str}")
        except Exception as e:
            print(f"[ERROR] Error creating provider path from key '{key}': {e}")
            provider_str = os.path.dirname(key)
            print(f"[ERROR] Fallback provider string: {provider_str}")

        try:
            self.s3_client.download_file(self.bucket, key, local_path)

            start_time = time.time()
            extracted_text = self.extract_pdf_text(Path(local_path), endpoint)
            duration = time.time() - start_time

            if extracted_text:
                result = self.save_extracted_markdown(
                    key, extracted_text,
                    self.save_to_local, self.bucket, self.destination_bucket
                )
                text_len = len(extracted_text)
                print(f"[INFO] Extracted {text_len} characters from {key}")
                print(f"[INFO] Processing time: {duration:.2f}s")
                if result:
                    print(f"[INFO] Successfully saved markdown for {key}")
                else:
                    print(f"[ERROR] Failed to save markdown for {key}")
            else:
                print(f"[ERROR] Empty extraction for {key}")

        except Exception as e:
            print(f"[ERROR] Exception processing {key}: {e}")
            import traceback
            traceback.print_exc()


    def _process_directory(self, directory_path, subdir_name):
        try:
            file_list = list(directory_path.glob('**/*'))
            files = [f for f in file_list if f.is_file()]
            print(f"Found {len(files)} files in {directory_path}")

            tasks = []
            for i, file_path in enumerate(files):
                file_extension = file_path.suffix.lower().lstrip('.')
                if file_extension == "pdf":
                    # Assign server in round-robin fashion
                    server = self.pdf_servers[i % len(self.pdf_servers)]
                    tasks.append((file_path, subdir_name, self.save_to_local,
                                self.bucket, self.destination_bucket, server))
                    
            if not tasks:
                print(f"No supported files found in {directory_path}")
                return

            # Process files in parallel
            total_start = time.time()
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self.process_pdf_file, *task) for task in tasks]

                for _ in tqdm(as_completed(futures), total=len(futures),
                              desc=f"Processing files in {subdir_name}"):
                    pass

            total_duration = time.time() - total_start
            print(f"Total processing time: {total_duration:.2f}s")

        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")

    @staticmethod
    def process_pdf_file(file_path, subdir_name, save_to_local, bucket, destination_bucket, endpoint):
        filename = file_path.name

        # Extract path relative to the subdirectory (e.g., 'mdpi')
        parts = file_path.parts
        subdir_index = parts.index(subdir_name)
        result_parts = parts[subdir_index:-1]
        provider_path = Path(*result_parts)

        try:
            print(f"[INFO] Started processing {file_path}...")

            key = str(file_path.relative_to(file_path.parent.parent)
                    if file_path.parent.parent != file_path.parent else file_path.name)

            start_time = time.time()
            extracted_text = DataExtractionS3Pipeline.extract_pdf_text(file_path, endpoint)
            duration = time.time() - start_time

            if extracted_text:
                result = DataExtractionS3Pipeline.save_extracted_markdown(
                    key, extracted_text, "PDF", provider_path,
                    save_to_local, bucket, destination_bucket
                )
                text_len = len(extracted_text)
                print(f"[INFO] Extracted {text_len} characters.")
                print(f"[INFO] Processing time: {duration:.2f}s")

                if result:
                    print(f"[INFO] Successfully saved markdown for {file_path}")
                else:
                    print(f"[ERROR] Failed to save markdown for {file_path}")
                return duration
            else:
                print(f"[ERROR] Text extraction failed (empty result) for {file_path}")
                print(f"[INFO] Processing time: {duration:.2f}s")
                return duration

        except Exception as e:
            duration = time.time() - start_time
            print(f"[ERROR] Error processing file {file_path}: {str(e)}")
            print(f"[INFO] Processing time: {duration:.2f}s")
            return duration


    @staticmethod
    def extract_pdf_text(file_path, endpoint):
        try:
            with open(file_path, "rb") as f:
                files = {
                    'file': (file_path.name, f, 'application/pdf')
                }
                headers = {
                    'accept': 'application/json'
                }

                print(f"[DEBUG] Posting {file_path} to {endpoint}")  # 

                response = requests.post(endpoint, headers=headers, files=files)

                print(f"[DEBUG] Response code: {response.status_code}")  # 
                print(f"[DEBUG] Response text: {response.text[:200]}")   # (Optional) preview first 200 chars

            return response.text
        except Exception as e:
            return None


    @staticmethod
    def process_text(text):
        # Remove the starting and ending quotes
        text = text.strip('"')

        # Replace the escaped "\n" with actual newline characters
        text = text.replace('\\n', '\n')
        return text

    @staticmethod
    def save_extracted_markdown(key, extracted_text, save_to_local, bucket,
                                destination_bucket):
        try:
            base_filename = DataExtractionS3Pipeline.get_safe_filename(key)
            # Construct S3 key: ensure destination_bucket is treated as prefix, no leading slash
            s3_key = f"{destination_bucket.strip('/')}/{base_filename}.md"

            # Clean the extracted text
            processed_text = DataExtractionS3Pipeline.process_text(extracted_text)

            if save_to_local:
                # Save locally — be sure the folder exists!
                local_folder = Path(destination_bucket)
                local_folder.mkdir(parents=True, exist_ok=True)
                local_file_path = local_folder / f"{base_filename}.md"
                with open(local_file_path, 'w', encoding='utf-8') as f:
                    f.write(processed_text)
                print(f"[INFO] Saved markdown locally: {local_file_path}")
            else:
                # Upload to S3
                client = boto3.client(
                    "s3",
                    region_name=os.getenv("AWS_REGION"),
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
                )
                print(f"[DEBUG] Uploading markdown to S3 bucket={bucket}, key={s3_key}")
                client.put_object(
                    Bucket=bucket,
                    Key=s3_key,
                    Body=processed_text.encode('utf-8'),
                    ContentType='text/markdown'
                )
                print(f"[INFO] Uploaded markdown to S3: s3://{bucket}/{s3_key}")

        except Exception as e:
            print(f"[ERROR] Error saving markdown: {str(e)}")
            return False

        
    @staticmethod
    def get_safe_filename(key):
        base_name = os.path.basename(key)
        base_name = os.path.splitext(base_name)[0]
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        return safe_name


import click

@click.command()
@click.option("--bucket", default="esa-satcom-s3", help="S3 bucket")
@click.option("--prefix", default="MS2/sample/pdfs/", help="S3 prefix to scan (recursively) for PDFs")
@click.option("--save-to-local", is_flag=True, help="Save extracted Markdown locally instead of uploading to S3")
@click.option("--destination-bucket", default="MS2/sample/no_ugat", help="S3 destination bucket (or folder if saving locally)")
@click.option("--max-workers", default=1, help="Number of parallel threads for processing")
def run_pipeline(bucket, prefix, save_to_local, destination_bucket, max_workers):
    extractor = DataExtractionS3Pipeline(
        bucket = bucket,
        prefix=prefix,
        save_to_local=save_to_local,
        destination_bucket=destination_bucket,
        max_workers=max_workers
    )
    extractor.process_files()

if __name__ == "__main__":
    run_pipeline()
