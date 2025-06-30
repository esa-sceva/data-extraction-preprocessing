import os
import boto3
from typing import Final
from pathlib import Path
import datetime
import time
import re

from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered

class DataExtractionS3Pipeline:
    def __init__(self, base_dir='', sub_folder='', save_to_local=False):
        self.base_dir = Path(base_dir)
        self.save_to_local = save_to_local
        self.destination_bucket = "marker_5k"
        self.sub_folder = sub_folder

        self.bucket_name: Final[str] = os.getenv("AWS_BUCKET_NAME", 'llm4eo-s3')

        print(f"[DEBUG] Initialized with base_dir={self.base_dir.resolve()}, sub_folder={self.sub_folder}, save_to_local={self.save_to_local}")

        if self.save_to_local:
            os.makedirs(f"{self.destination_bucket}", exist_ok=True)
            if self.sub_folder:
                self._setup_directories(self.sub_folder)
        else:
            print("[DEBUG] S3 saving mode enabled")

        self.model = PdfConverter(artifact_dict=create_model_dict())
        print("[DEBUG] Marker PDF model initialized")

    def _setup_directories(self, sub_folder):
        os.makedirs(f"{self.destination_bucket}/{sub_folder}", exist_ok=True)

    def process_files(self):
        try:
            if not self.sub_folder:
                subdirs = self._discover_subdirectories()
                for subdir in subdirs:
                    print(f"[DEBUG] Processing subdir: {subdir}")
                    if self.save_to_local:
                        self._setup_directories(subdir)
                    subdir_path = self.base_dir / subdir
                    self._process_directory(subdir_path, subdir)
            else:
                self._process_directory(self.base_dir, self.sub_folder)
        except Exception as e:
            print(f"[ERROR] process_files failed: {e}")

    def _discover_subdirectories(self):
        subdirs = []
        try:
            for path in self.base_dir.iterdir():
                if path.is_dir():
                    subdirs.append(path.name)
            if not subdirs:
                subdirs = [self.base_dir.name]
        except Exception as e:
            print(f"[ERROR] _discover_subdirectories: {e}")
        return subdirs

    def _process_directory(self, directory_path, subdir_name):
        try:
            file_list = list(directory_path.glob('**/*'))
            files = [f for f in file_list if f.is_file()]
            print(f"[DEBUG] Found {len(files)} files in {directory_path.resolve()}")

            total_start = time.time()
            processed_count = 0

            for file_path in files:
                print(f"[DEBUG] Inspecting: {file_path}")
                ext = file_path.suffix.lower().lstrip('.')
                if ext == "pdf":
                    print(f"[DEBUG] Processing {processed_count+1}/{len(files)}: {file_path.name}")
                    duration = self.process_pdf_file(
                        file_path, subdir_name,
                        self.save_to_local, self.bucket_name,
                        self.destination_bucket
                    )
                    print(f"[DEBUG] Done in {duration:.2f}s")
                    processed_count += 1
                else:
                    print(f"[SKIP] Unsupported file type ({ext}): {file_path.name}")
            print(f"[DEBUG] Finished {processed_count} PDFs in {time.time() - total_start:.2f}s")
        except Exception as e:
            print(f"[ERROR] _process_directory: {e}")

    def process_pdf_file(self, file_path, subdir_name, save_to_local, bucket_name, destination_bucket):
        start_time = time.time()
        try:
            # Construct a consistent key
            rel_key = str(file_path.relative_to(self.base_dir))
            print(f"[DEBUG] Relative key: {rel_key}")

            extracted_text = self.extract_pdf_text(file_path)
            if not extracted_text:
                print(f"[WARN] Empty extraction: {file_path.name}")
                return time.time() - start_time

            result = self.save_extracted_markdown(
                rel_key, extracted_text, "PDF", str(subdir_name),
                save_to_local, bucket_name, destination_bucket
            )
            if result:
                print(f"[DEBUG] Saved markdown for: {file_path.name}")
            else:
                print(f"[ERROR] Save failed for: {file_path.name}")
            return time.time() - start_time
        except Exception as e:
            print(f"[ERROR] process_pdf_file: {e}")
            return time.time() - start_time

    def extract_pdf_text(self, file_path):
        try:
            rendered = self.model(str(file_path))
            text, _, _ = text_from_rendered(rendered)
            print(f"[DEBUG] Extracted {len(text)} characters from {file_path.name}")
            return text
        except Exception as e:
            print(f"[ERROR] extract_pdf_text: {e}")
            return None

    @staticmethod
    def process_text(text):
        return text.strip('"').replace('\\n', '\n')

    @staticmethod
    def save_extracted_markdown(key, extracted_text, file_type, subdir_name,
                                 save_to_local, bucket_name, destination_bucket):
        try:
            base_filename = DataExtractionS3Pipeline.get_safe_filename(key)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            s3_key = f"{destination_bucket}/{subdir_name}/{base_filename}.md"
            processed_text = DataExtractionS3Pipeline.process_text(extracted_text)

            if save_to_local:
                os.makedirs(os.path.dirname(s3_key), exist_ok=True)
                with open(s3_key, 'w', encoding='utf-8') as f:
                    f.write(processed_text)
                print(f"[DEBUG] Saved locally: {s3_key}")
            else:
                client = boto3.client(
                    "s3",
                    region_name=os.getenv("AWS_REGION"),
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
                )
                response = client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=processed_text.encode('utf-8'),
                    ContentType='text/markdown'
                )
                status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                print(f"[DEBUG] S3 upload status for {s3_key}: {status_code}")
                if status_code != 200:
                    raise RuntimeError(f"S3 put_object failed with status {status_code}")
            return True
        except Exception as e:
            print(f"[ERROR] save_extracted_markdown: {e}")
            return False

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
