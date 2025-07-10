"""
This code uses multithreading to extract text from PDF only using nougat. This setup assumes 4 fastapi servers and requests distributed
in round robin fashion.
"""
import os
import boto3
from typing import Final
from pathlib import Path
from tqdm.auto import tqdm
import mysql.connector
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import tempfile
from log_db import Severity, LogEntry
import re
import requests

class DataExtractionS3Pipeline:
    def __init__(self, save_to_local=False, max_workers=4, destination_bucket="raw_data_dedup_extractions"):
        self.bucket_name = "esa-satcom-s3"
        self.prefix = "MS2/sample/pdfs/"
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
            "http://127.0.0.1:8001/predict/",
            "http://127.0.0.1:8003/predict/",
            "http://127.0.0.1:8004/predict/",
            "http://127.0.0.1:8005/predict/"
        ]

        print(f"Reading PDFs from s3://{self.bucket_name}/{self.prefix}")



    def _setup_directories(self, sub_folder):
        os.makedirs(f"{self.destination_bucket}/{sub_folder}", exist_ok=True)

    def process_files(self):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix)

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
            self.s3_client.download_file(self.bucket_name, key, local_path)

            start_time = time.time()
            extracted_text = self.extract_pdf_text(Path(local_path), endpoint, log_entry=None)
            duration = time.time() - start_time

            if extracted_text:
                result = self.save_extracted_markdown(
                    key, extracted_text, "PDF", provider_str,
                    self.save_to_local, self.bucket_name, self.destination_bucket, log_entry=None
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



    def get_successful_files_by_provider(self, provider):
        """
        Get all files with status 'success' for a specific provider from the database

        Parameters:
            provider (str): The provider name to filter by

        Returns:
            list: List of files with status 'success' for the specified provider
        """
        # Database connection parameters
        db_host = os.getenv("DB_HOST")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        db_config = {
            'host': db_host,
            'user': db_user,
            'password': db_password,
            'database': db_name
        }
        try:
            # Establish database connection
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor(dictionary=True)

            # Execute query with parameter binding for security
            query = """
                    SELECT filename, log, provider
                    FROM text_extraction_logs
                    WHERE provider LIKE %s
                    AND status = 'success' \
                    """
            cursor.execute(query, (f'{provider}%',))

            # Fetch all matching records
            results = cursor.fetchall()

            # Filter according to the error in log
            error_log_str = 'Error saving markdown: Unable to locate credentials'

            # Filter out the logs that contain the error
            results = [result for result in results if error_log_str not in result['log']]

            results = [f"{result['provider']}/{result['filename']}" for result in results]

            cursor.close()
            conn.close()

            return results

        except mysql.connector.Error as err:
            print(f"Database error: {err}")
            return []

    def get_relative_provider_path(self, filepath: Path, subdir_name: str) -> Path:
        """
        Get the relative path of the provider from the file path.

        Parameters:
            file_path (str): The full file path

        Returns:
            str: The relative path of the provider
        """
        parts = filepath.parts
        subfolder_index = parts.index(subdir_name)
        result_parts = parts[subfolder_index:]
        return Path(*result_parts)



    def _process_directory(self, directory_path, subdir_name):
        try:
            file_list = list(directory_path.glob('**/*'))
            files = [f for f in file_list if f.is_file()]
            print(f"Found {len(files)} files in {directory_path}")

            # Remove files that have already been processed successfully
            successful_files = self.get_successful_files_by_provider(subdir_name)
            print('Files already processed successfully:', len(successful_files))
            files = [f for f in files if self.get_relative_provider_path(f, subdir_name).as_posix() not in successful_files]
            print(f"Remaining files to process: {len(files)}")

            tasks = []
            for i, file_path in enumerate(files):
                file_extension = file_path.suffix.lower().lstrip('.')
                if file_extension == "pdf":
                    # Assign server in round-robin fashion
                    server = self.pdf_servers[i % len(self.pdf_servers)]
                    tasks.append((file_path, subdir_name, self.save_to_local,
                                self.bucket_name, self.destination_bucket, server))
                else:
                    log_entry = LogEntry.start_new(file_path.name, provider=subdir_name,
                                                log_text=f'Started processing {file_path}...',
                                                file_path=file_path)
                    log_entry.log(f"Unsupported file type: {file_extension}", severity=Severity.ERROR)
                    log_entry.finalize_log("error")

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
    def process_pdf_file(file_path, subdir_name, save_to_local, bucket_name, destination_bucket, endpoint):
        filename = file_path.name
        # Get the path after the subdir_name
        # Find the index of 'mdpi' in the path parts
        parts = file_path.parts
        subdir_index = parts.index(subdir_name)

        # Extract the parts starting from 'mdpi'
        result_parts = parts[subdir_index:-1]

        # Join them back into a path
        provider_path = Path(*result_parts)
        try:
            log_entry = LogEntry.start_new(filename, provider=provider_path.as_posix(),
                                           log_text=f'Started processing {file_path}...',
                                           file_path=file_path)


            key = str(file_path.relative_to(file_path.parent.parent)
                      if file_path.parent.parent != file_path.parent else file_path.name)
            start_time = time.time()
            extracted_text = DataExtractionS3Pipeline.extract_pdf_text(file_path, endpoint, log_entry)
            duration = time.time() - start_time
            if extracted_text:
                result = DataExtractionS3Pipeline.save_extracted_markdown(
                    key, extracted_text, "PDF", provider_path, save_to_local, bucket_name, destination_bucket, log_entry)
                text_len = len(extracted_text)
                log_entry.log(f"Extracted {text_len} characters.")
                log_entry.log(f"Processing time: {duration:.2f}s")
                if result:
                    log_entry.finalize_log("success", text_len, duration)
                else:
                    log_entry.finalize_log("error", text_len, duration)
                return duration
            else:
                duration = time.time() - start_time
                log_entry.log("Text extraction failed (empty result).", severity=Severity.ERROR)
                log_entry.log(f"Processing time: {duration:.2f}s", severity=Severity.ERROR)
                log_entry.finalize_log("error", 0)
                return duration

        except Exception as e:
            duration = time.time() - start_time
            log_entry.log(f"Error processing file: {str(e)}", severity=Severity.ERROR)
            log_entry.log(f"Processing time: {duration:.2f}s", severity=Severity.ERROR)
            log_entry.finalize_log("error")
            return duration

    @staticmethod
    def extract_pdf_text(file_path, endpoint, log_entry=None):
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

                if log_entry:
                    log_entry.log(f"Used PDF server: {endpoint}")

                if response.status_code != 200:
                    if log_entry:
                        log_entry.log(f"PDF server returned status code {response.status_code}",
                                    severity=Severity.ERROR)
                    return None

            return response.text
        except Exception as e:
            if log_entry:
                log_entry.log(f"PDF extraction error: {str(e)}", severity=Severity.ERROR)
            return None


    @staticmethod
    def process_text(text):
        # Remove the starting and ending quotes
        text = text.strip('"')

        # Replace the escaped "\n" with actual newline characters
        text = text.replace('\\n', '\n')
        return text

    @staticmethod
    def save_extracted_markdown(key, extracted_text, file_type, subdir_name, save_to_local, bucket_name,
                                destination_bucket, log_entry=None):
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
                print(f"[DEBUG] Uploading markdown to S3 bucket={bucket_name}, key={s3_key}")
                client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=processed_text.encode('utf-8'),
                    ContentType='text/markdown'
                )
                print(f"[INFO] Uploaded markdown to S3: s3://{bucket_name}/{s3_key}")

            if log_entry:
                log_entry.log(f"Saved markdown to {'local' if save_to_local else 'S3'}: {s3_key if not save_to_local else local_file_path}")
            return True
        except Exception as e:
            if log_entry:
                log_entry.log(f"Error saving markdown: {str(e)}", severity=Severity.ERROR)
            print(f"[ERROR] Error saving markdown: {str(e)}")
            return False

        
    @staticmethod
    def get_safe_filename(key):
        base_name = os.path.basename(key)
        base_name = os.path.splitext(base_name)[0]
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        return safe_name


if __name__ == '__main__':
    extractor = DataExtractionS3Pipeline(
        save_to_local=False,
        destination_bucket="MS2/sample/nougat" 
    )
    extractor.process_files()

