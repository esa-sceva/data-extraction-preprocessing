"""
This code uses sequential processing to handle only html, txt and json.
"""

import os
import boto3
from typing import Final
from bs4 import BeautifulSoup
from pathlib import Path

from tqdm.auto import tqdm
import datetime
import json
from log_db import Severity, LogEntry


class DataExtractionS3Pipeline:
    def __init__(self, base_dir='', sub_folder='', save_to_local=False):
        self.base_dir = Path(base_dir)
        self.save_to_local = save_to_local
        self.destination_bucket = "raw_data_dedup_extractions"
        self.sub_folder = sub_folder

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

    def _process_directory(self, directory_path, subdir_name):
        try:
            file_list = list(directory_path.glob('**/*'))
            files = [f for f in file_list if f.is_file()]
            print(f"Found {len(file_list)} files in {directory_path}")

            # Sequential processing instead of multiprocessing
            for file in tqdm(files, desc=f"Processing files in {subdir_name}"):
                self.process_file(
                    file_path=file,
                    subdir_name=subdir_name,
                    save_to_local=self.save_to_local,
                    bucket_name=self.bucket_name,
                    destination_bucket=self.destination_bucket
                )

        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")

    def process_file(self, file_path, subdir_name, save_to_local, bucket_name, destination_bucket):
        filename = file_path.name
        try:
            log_entry = LogEntry.start_new(filename, provider=subdir_name, log_text=f'Started processing {file_path}...', file_path=file_path)

            file_extension = file_path.suffix.lower().lstrip('.')
            key = str(file_path.relative_to(file_path.parent.parent)
                      if file_path.parent.parent != file_path.parent else file_path.name)

            if file_extension == "html":
                extracted_text = self.extract_html_text(file_path, log_entry)
                file_type = "HTML"
            elif file_extension == "txt":
                extracted_text = self.extract_txt_text(file_path, log_entry)
                file_type = "TXT"
            elif file_extension == "json":
                extracted_text = self.extract_json_text(file_path, log_entry)
                file_type = "JSON"
            else:
                log_entry.log(f"Unsupported file type: {file_extension}", severity=Severity.ERROR)
                log_entry.finalize_log("error")
                return

            if extracted_text:
                self.save_extracted_markdown(
                    key, extracted_text, file_type, subdir_name, save_to_local, bucket_name, destination_bucket, log_entry)
                text_len = len(extracted_text)
                log_entry.log(f"Extracted {text_len} characters.")
                log_entry.finalize_log("success", text_len)
            else:
                log_entry.log("Text extraction failed (empty result).", severity=Severity.ERROR)
                log_entry.finalize_log("error", 0)

        except Exception as e:
            log_entry.log(f"Error processing file: {str(e)}", severity=Severity.ERROR)
            log_entry.finalize_log("error")

    @staticmethod
    def extract_html_text(file_path, log_entry=None):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                soup = BeautifulSoup(file, 'html.parser')
                text = soup.get_text().strip()
                text = os.linesep.join([s for s in text.splitlines() if s.strip()])
            return text
        except Exception as e:
            if log_entry:
                log_entry.log(f"HTML extraction error: {str(e)}", severity=Severity.ERROR)
            return None

    @staticmethod
    def extract_txt_text(file_path, log_entry=None):
        try:
            encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            text = None

            for encoding in encodings_to_try:
                try:
                    with open(file_path, 'r', encoding=encoding) as file:
                        text = file.read()
                    log_entry.log(f"Successfully read file with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue

            return text
        except Exception as e:
            if log_entry:
                log_entry.log(f"TXT extraction error: {str(e)}")
            return None

    @staticmethod
    def extract_json_text(file_path, log_entry=None):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                raw_content = file.read()
            try:
                json_data = json.loads(raw_content)
                text = json.dumps(json_data, indent=2)
            except json.JSONDecodeError as json_err:
                text = raw_content
                log_entry.log(f"JSON parsing error: {str(json_err)}. Using raw content.", severity=Severity.ERROR)
            return text
        except Exception as e:
            if log_entry:
                log_entry.log(f"JSON extraction error: {str(e)}", severity=Severity.ERROR)
            return None

    @staticmethod
    def save_extracted_markdown(key, extracted_text, file_type, subdir_name, save_to_local, bucket_name, destination_bucket, log_entry=None):
        try:
            base_filename = DataExtractionS3Pipeline.get_safe_filename(key)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file_path = f"{destination_bucket}/{subdir_name}/{base_filename}.md"

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
        import re
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        return safe_name

if __name__ == '__main__':
    extractor = DataExtractionS3Pipeline(
        base_dir='data',
        save_to_local=True,
    )
    extractor.process_files()