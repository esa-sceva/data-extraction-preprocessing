"""
This code uses sequential processing to process files using marker for ocr.
"""
import os
import boto3
from typing import Final
from pathlib import Path
import datetime
import time
import re

from log_db import Severity, LogEntry

from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered

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
        
        self.model = PdfConverter(
            artifact_dict=create_model_dict(),
        )
        print("Initialized the model.")

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

            # Process files sequentially
            total_start = time.time()
            processed_count = 0
            
            print(f"Starting sequential processing for {len(files)} files in {subdir_name}")
            for file_path in files:
                file_extension = file_path.suffix.lower().lstrip('.')
                if file_extension == "pdf":
                    print(f"Processing {processed_count+1}/{len(files)}: {file_path.name}")
                    processing_time = self.process_pdf_file(
                        file_path, 
                        subdir_name, 
                        self.save_to_local,
                        self.bucket_name, 
                        self.destination_bucket, 
                    )
                    processed_count += 1
                    print(f"Completed in {processing_time:.2f}s")
                else:
                    log_entry = LogEntry.start_new(
                        file_path.name, 
                        provider=subdir_name,
                        log_text=f'Started processing {file_path}...',
                        file_path=file_path
                    )
                    log_entry.log(f"Unsupported file type: {file_extension}", severity=Severity.ERROR)
                    log_entry.finalize_log("error")

            total_duration = time.time() - total_start
            print(f"Total processing time: {total_duration:.2f}s for {processed_count} files")

        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")

    def process_pdf_file(self, file_path, subdir_name, save_to_local, bucket_name, destination_bucket):
        filename = file_path.name
        # Get the path after the subdir_name
        parts = file_path.parts
        subdir_index = parts.index(subdir_name)

        # Extract the parts starting from subdir_name
        result_parts = parts[subdir_index:-1]

        # Join them back into a path
        provider_path = Path(*result_parts)
        start_time = time.time()
        
        try:
            log_entry = LogEntry.start_new(
                filename, 
                provider=provider_path.as_posix(),
                log_text=f'Started processing {file_path}...',
                file_path=file_path
            )

            key = str(file_path.relative_to(file_path.parent.parent)
                    if file_path.parent.parent != file_path.parent else file_path.name)
            
            extracted_text = self.extract_pdf_text(file_path, log_entry)
            duration = time.time() - start_time
            
            if extracted_text:
                result = self.save_extracted_markdown(
                    key, 
                    extracted_text, 
                    "PDF", 
                    provider_path, 
                    save_to_local, 
                    bucket_name, 
                    destination_bucket, 
                    log_entry
                )
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
            if 'log_entry' in locals():
                log_entry.log(f"Error processing file: {str(e)}", severity=Severity.ERROR)
                log_entry.log(f"Processing time: {duration:.2f}s", severity=Severity.ERROR)
                log_entry.finalize_log("error")
            else:
                print(f"Error processing file {file_path}: {str(e)}")
            return duration

    def extract_pdf_text(self, file_path, log_entry=None):
        try:
            rendered = self.model(str(file_path)) # explicit conversion to str since pydantic base throws an error
            text, _, _ = text_from_rendered(rendered)
            return text
        except Exception as e:
            #print(f"PDF extraction error: {str(e)}")
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
            return True
        except Exception as e:
            if log_entry:
                log_entry.log(f"Error saving markdown: {str(e)}", severity=Severity.ERROR)
            return False

    @staticmethod
    def get_safe_filename(key):
        base_name = os.path.basename(key)
        base_name = os.path.splitext(base_name)[0]
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        return safe_name


if __name__ == '__main__':
    extractor = DataExtractionS3Pipeline(
        base_dir='data2',
        save_to_local=False,
    )
    extractor.process_files()