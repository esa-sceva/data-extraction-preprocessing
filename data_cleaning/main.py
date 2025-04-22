"""
This script is used for data cleaning.

1. Removal of nougat artifacts ([MISSIN_PAGE], WARNING from pdf files. , it seems to be enclosed between +++
2. Remove the files with < 1000 characters.
3. Remove repeated punctuations.
4. Remove table of contents ellipses thingy.

"""

import os
import boto3
from typing import Final
from pathlib import Path
import re
from tqdm.auto import tqdm
from multiprocessing import Pool, Manager
from functools import partial
from datetime import datetime


class SimpleLogger:
    def __init__(self, filename):
        self.log_path = Path("logs")
        self.log_path.mkdir(exist_ok=True)
        self.file = self.log_path / f"{filename}.log"

    def log(self, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.file, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {message}\n")


class MarkdownCleaningPipeline:
    def __init__(self, base_dir='', sub_folder='', save_to_local=False, num_processes=None):
        self.base_dir = Path(base_dir)
        self.save_to_local = save_to_local
        self.destination_bucket = "raw_data_dedup_cleaned"
        self.sub_folder = sub_folder
        self.num_processes = num_processes if num_processes else os.cpu_count()

        self.bucket_name: Final[str] = os.getenv("AWS_BUCKET_NAME", 'llm4eo-s3')
        if self.save_to_local:
            print("Saving cleaned markdown files to local directory")
            os.makedirs(f"{self.destination_bucket}", exist_ok=True)
            if self.sub_folder:
                self._setup_directories(self.sub_folder)
        else:
            print("Saving cleaned markdown to S3")

        self.manager = Manager()

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
            markdown_files = list(directory_path.glob('**/*.md'))
            print(f"Found {len(markdown_files)} files in {directory_path}")

            with Pool(processes=self.num_processes) as pool:
                process_func = partial(self.process_file_wrapper,
                                       subdir_name=subdir_name,
                                       save_to_local=self.save_to_local,
                                       bucket_name=self.bucket_name,
                                       destination_bucket=self.destination_bucket)
                list(tqdm(pool.imap(process_func, markdown_files),
                          total=len(markdown_files),
                          desc=f"Cleaning markdown files in {subdir_name}"))

        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")

    @staticmethod
    def process_file_wrapper(file_path, subdir_name, save_to_local, bucket_name, destination_bucket):
        return MarkdownCleaningPipeline.process_file_static(
            file_path, subdir_name, save_to_local, bucket_name, destination_bucket)

    @staticmethod
    def process_file_static(file_path, subdir_name, save_to_local, bucket_name, destination_bucket):
        filename = file_path.name
        logger = SimpleLogger(subdir_name)

        try:
            logger.log(f"[START] Cleaning {filename}")
            key = str(file_path.relative_to(file_path.parent.parent)
                      if file_path.parent.parent != file_path.parent else file_path.name)

            markdown_content = MarkdownCleaningPipeline.read_markdown_file(file_path)

            if markdown_content:
                cleaned_markdown = MarkdownCleaningPipeline.clean_markdown(markdown_content)

                if len(cleaned_markdown) < 100:
                    logger.log(f"[SKIP] {filename} - Less than 100 characters after cleaning")
                    return

                MarkdownCleaningPipeline.save_cleaned_markdown(
                    key, cleaned_markdown, subdir_name, save_to_local, bucket_name, destination_bucket, logger)
                logger.log(f"[SUCCESS] Cleaned and saved {filename} - {len(cleaned_markdown)} characters")
            else:
                logger.log(f"[ERROR] {filename} - Could not read markdown content")

        except Exception as e:
            logger.log(f"[ERROR] {filename} - Exception during cleaning: {str(e)}")

    @staticmethod
    def read_markdown_file(file_path):
        try:
            encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            for encoding in encodings_to_try:
                try:
                    with open(file_path, 'r', encoding=encoding) as file:
                        return file.read()
                except UnicodeDecodeError:
                    continue
            return None
        except Exception:
            return None

    @staticmethod
    def clean_markdown(markdown_text):
        if not markdown_text:
            return ""

        try:
            cleaned_text = re.sub(r'\+\+\+\s*==WARNING: Truncated because of repetitions==.*?\+\+\+',
                                  '', markdown_text, flags=re.DOTALL)
            cleaned_text = re.sub(r'\+\+\+\s*==ERROR: No output for this page==.*?\+\+\+',
                                  '', cleaned_text, flags=re.DOTALL)
            cleaned_text = re.sub(r'\.{3,}', ' ', cleaned_text)
            cleaned_text = re.sub(r'([!?.,])\1{2,}', r'\1\1', cleaned_text)
            return cleaned_text
        except Exception:
            return markdown_text

    @staticmethod
    def save_cleaned_markdown(key, cleaned_markdown, subdir_name, save_to_local, bucket_name, destination_bucket, logger):
        try:
            base_filename = MarkdownCleaningPipeline.get_safe_filename(key)
            file_path = f"{destination_bucket}/{subdir_name}/{base_filename}.md"

            if save_to_local:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(cleaned_markdown)
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
                    Body=cleaned_markdown.encode('utf-8'),
                    ContentType='text/markdown'
                )
            logger.log(f"[SAVE] File saved to {'local' if save_to_local else 'S3'}: {file_path}")
        except Exception as e:
            logger.log(f"[ERROR] Failed to save cleaned file: {str(e)}")

    @staticmethod
    def get_safe_filename(key):
        base_name = os.path.basename(key)
        base_name = os.path.splitext(base_name)[0]
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        return safe_name


if __name__ == '__main__':
    cleaner = MarkdownCleaningPipeline(
        base_dir='data',
        save_to_local=True,
    )
    cleaner.process_files()
