import os
import boto3
from typing import Final
from pathlib import Path
from tqdm.auto import tqdm
import mysql.connector
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from log_db import Severity, LogEntry
import re
import requests


class DataExtractionS3Pipeline:
    def __init__(self, base_dir='', sub_folder='', save_to_local=False, max_workers=None):
        self.base_dir = Path(base_dir)
        self.save_to_local = save_to_local
        self.destination_bucket = "raw_data_dedup_extractions"
        self.sub_folder = sub_folder
        self.max_workers = 4

        self.pdf_servers = [
            "http://127.0.0.1:8002/predict/",
            "http://127.0.0.1:8003/predict/",
            "http://127.0.0.1:8004/predict/",
            "http://127.0.0.1:8005/predict/"
        ]

        self.bucket_name: Final[str] = os.getenv("AWS_BUCKET_NAME", 'llm4eo-s3')
        if self.save_to_local:
            print("Saving files to local directory")
            os.makedirs(f"{self.destination_bucket}", exist_ok=True)
            if self.sub_folder:
                self._setup_directories(self.sub_folder)
        else:
            print("Saving to S3")

    def _setup_directories(self, sub_folder):
        os.makedirs(f"{self.destination_bucket}/{sub_folder}", exist_ok=True)

    def process_files(self):
        try:
            if not self.sub_folder:
                subdirs = self._discover_subdirectories()
                for subdir in subdirs:
                    print(f"Processing subdirectory: {subdir}")
                    if self.save_to_local:
                        self._setup_directories(subdir)
                    subdir_path = self.base_dir / subdir
                    self._process_directory(subdir_path, subdir)
            else:
                self._process_directory(self.base_dir, self.sub_folder)

        except Exception as e:
            print(f"Error processing files: {str(e)}")

    def _discover_subdirectories(self):
        subdirs = []
        try:
            for path in self.base_dir.iterdir():
                if path.is_dir():
                    subdirs.append(path.name)
            if not subdirs:
                subdirs = [self.base_dir.name]
        except Exception as e:
            print(f"Error discovering subdirectories: {str(e)}")
        return subdirs

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
                    SELECT filename
                    FROM text_extraction_logs
                    WHERE provider = %s
                      AND status = 'success' \
                    """
            cursor.execute(query, (provider,))

            # Fetch all matching records
            results = cursor.fetchall()
            results = [result['filename'] for result in results]

            cursor.close()
            conn.close()

            return results

        except mysql.connector.Error as err:
            print(f"Database error: {err}")
            return []

    def _process_directory(self, directory_path, subdir_name):
        try:
            file_list = list(directory_path.glob('**/*'))
            files = [f for f in file_list if f.is_file()]
            print(f"Found {len(files)} files in {directory_path}")

            # Remove files that have already been processed successfully
            successful_files = self.get_successful_files_by_provider(subdir_name)
            print('Files already processed successfully:', len(successful_files))
            files = [f for f in files if f.name not in successful_files]
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
        try:
            log_entry = LogEntry.start_new(filename, provider=subdir_name,
                                           log_text=f'Started processing {file_path}...',
                                           file_path=file_path)


            key = str(file_path.relative_to(file_path.parent.parent)
                      if file_path.parent.parent != file_path.parent else file_path.name)
            start_time = time.time()
            extracted_text = DataExtractionS3Pipeline.extract_pdf_text(file_path, endpoint, log_entry)
            duration = time.time() - start_time
            if extracted_text:
                DataExtractionS3Pipeline.save_extracted_markdown(
                    key, extracted_text, "PDF", subdir_name, save_to_local, bucket_name, destination_bucket, log_entry)
                text_len = len(extracted_text)
                log_entry.log(f"Extracted {text_len} characters.")
                log_entry.log(f"Processing time: {duration:.2f}s")
                log_entry.finalize_log("success", text_len, duration)
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
                response = requests.post(endpoint, headers=headers, files=files)

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
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file_path = f"{destination_bucket}/{subdir_name}/{base_filename}.md"

            # Process the extracted text to remove unwanted characters
            extracted_text = DataExtractionS3Pipeline.process_text(extracted_text)

            if save_to_local:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(extracted_text)
            else:
                client = boto3.client(
                    "s3",
                    region_name=os.getenv("AWS_REGION"),
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
                )
                client.put_object(
                    Bucket=bucket_name,
                    Key=file_path,
                    Body=extracted_text.encode('utf-8'),
                    ContentType='text/markdown'
                )
            if log_entry:
                log_entry.log(f"Saved markdown to {'local' if save_to_local else 'S3'}: {file_path}")
        except Exception as e:
            if log_entry:
                log_entry.log(f"Error saving markdown: {str(e)}", severity=Severity.ERROR)

    @staticmethod
    def get_safe_filename(key):
        base_name = os.path.basename(key)
        base_name = os.path.splitext(base_name)[0]
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        return safe_name


if __name__ == '__main__':
    extractor = DataExtractionS3Pipeline(
        base_dir='data',
        save_to_local=False,
    )
    extractor.process_files()
