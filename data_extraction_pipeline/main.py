import os
import io
import boto3
import pandas as pd
from typing import Final
from PyPDF2 import PdfReader
from bs4 import BeautifulSoup
import logging
from pathlib import Path
from tqdm.auto import tqdm
import datetime
import json
from multiprocessing import Pool, Manager
from functools import partial

class DataExtractionS3Pipeline:
    def __init__(self, base_dir='', sub_folder='', save_to_local=False, num_processes=None):
        """Initialize the data extraction pipeline with options for local or S3 storage"""
        self.base_dir = Path(base_dir)
        self.save_to_local = save_to_local
        self.destination_bucket = "raw_data_dedup_extractions"
        self.sub_folder = sub_folder
        self.num_processes = num_processes if num_processes else os.cpu_count()
        
        self.bucket_name: Final[str] = os.getenv("AWS_BUCKET_NAME", 'llm4eo-s3')
        
        if self.save_to_local:
            print("Saving files to local directory")
            os.makedirs(f"{self.destination_bucket}", exist_ok=True)
            if self.sub_folder:
                self._setup_directories(self.sub_folder)
        else:
            print("Saving to S3")

        self.manager = Manager()
        
        self.log_file = "extraction_pipeline.log"
        logging.basicConfig(filename=self.log_file, level=logging.INFO,
                          format='%(asctime)s - %(message)s')

    def _setup_directories(self, sub_folder):
        """Setup necessary directories for a given subfolder"""
        os.makedirs(f"{self.destination_bucket}/{sub_folder}", exist_ok=True)

    def process_files(self):
        """Process all files using multiprocessing"""
        try:
            if not self.sub_folder:
                subdirs = self._discover_subdirectories()
                for subdir in subdirs:
                    print(f"Processing subdirectory: {subdir}")
                    logging.info(f"Processing subdirectory: {subdir}")
                    if self.save_to_local:
                        self._setup_directories(subdir)
                    subdir_path = self.base_dir / subdir
                    self._process_directory(subdir_path, subdir)
            else:
                self._process_directory(self.base_dir, self.sub_folder)
            
        except Exception as e:
            print(f"Error processing files: {str(e)}")
            logging.error(f"Error processing files: {str(e)}")

    def _discover_subdirectories(self):
        """Discover all subdirectories in the base directory"""
        subdirs = []
        try:
            for path in self.base_dir.iterdir():
                if path.is_dir():
                    subdirs.append(path.name)
            if not subdirs:
                subdirs = [self.base_dir.name]
            logging.info(f"Discovered subdirectories: {', '.join(subdirs)}")
        except Exception as e:
            logging.error(f"Error discovering subdirectories: {str(e)}")
            print(f"Error discovering subdirectories: {str(e)}")
        return subdirs

    def _process_directory(self, directory_path, subdir_name):
        """Process all files in a directory using multiprocessing"""
        try:
            file_list = list(directory_path.glob('**/*'))
            files = [f for f in file_list if f.is_file()]
            
            with Pool(processes=self.num_processes) as pool:
                process_func = partial(self.process_file_wrapper, 
                                     subdir_name=subdir_name,
                                     save_to_local=self.save_to_local,
                                     bucket_name=self.bucket_name,
                                     destination_bucket=self.destination_bucket)
                list(tqdm(pool.imap(process_func, files), 
                        total=len(files), 
                        desc=f"Processing files in {subdir_name}"))
            
        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")
            logging.error(f"Error processing directory {directory_path}: {str(e)}")

    @staticmethod
    def process_file_wrapper(file_path, subdir_name, save_to_local, bucket_name, destination_bucket):
        """Wrapper function for multiprocessing"""
        rel_path = file_path.relative_to(file_path.parent.parent) if file_path.parent.parent != file_path.parent else file_path.name
        logging.info(f"Processing file: {rel_path}")
        return DataExtractionS3Pipeline.process_file_static(
            file_path, subdir_name, save_to_local, bucket_name, destination_bucket)

    @staticmethod
    def process_file_static(file_path, subdir_name, save_to_local, bucket_name, destination_bucket):
        """Static method to process a single file"""
        try:
            file_extension = file_path.suffix.lower().lstrip('.')
            key = str(file_path.relative_to(file_path.parent.parent) if file_path.parent.parent != file_path.parent else file_path.name)
            
            if file_extension == "pdf":
                extracted_text = DataExtractionS3Pipeline.extract_pdf_text(file_path)
                file_type = "PDF"
            elif file_extension == "html":
                extracted_text = DataExtractionS3Pipeline.extract_html_text(file_path)
                file_type = "HTML"
            elif file_extension == "txt":
                extracted_text = DataExtractionS3Pipeline.extract_txt_text(file_path)
                file_type = "Text"
            elif file_extension == "json":
                extracted_text = DataExtractionS3Pipeline.extract_json_text(file_path)
                file_type = "JSON"
            else:
                logging.info(f"Unsupported file type: {file_extension}")
                return
                
            if extracted_text:
                DataExtractionS3Pipeline.save_extracted_markdown(
                    key, extracted_text, file_type, subdir_name, save_to_local, bucket_name, destination_bucket)
            else:
                pass
                #this is probably where we need the fallback
                
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            logging.error(f"Error processing file {file_path}: {str(e)}")

    @staticmethod
    def extract_pdf_text(file_path):
        """Extract text from PDF file"""
        try:
            with open(file_path, 'rb') as file:
                reader = PdfReader(file)
                text = "".join(page.extract_text() or "" for page in reader.pages)
            return text
        except Exception as e:
            logging.error(f"Error extracting PDF text: {str(e)}")
            return None

    @staticmethod
    def extract_html_text(file_path):
        """Extract text from HTML file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                soup = BeautifulSoup(file, 'html.parser')
                text = soup.get_text().strip()
                text = os.linesep.join([s for s in text.splitlines() if s.strip()])
            return text
        except Exception as e:
            logging.error(f"Error extracting HTML text: {str(e)}")
            return None
    
    @staticmethod
    def extract_txt_text(file_path):
        """Extract text from TXT file"""
        try:
            # Try different encodings in order
            encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            text = None
            
            for encoding in encodings_to_try:
                try:
                    with open(file_path, 'r', encoding=encoding) as file:
                        text = file.read()
                    logging.info(f"Successfully read file with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            return text
        except Exception as e:
            logging.error(f"Error extracting TXT text: {str(e)}")
            return None

    @staticmethod
    def extract_json_text(file_path):
        """Extract text from JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                raw_content = file.read()
                
            try:
                json_data = json.loads(raw_content)
                text = json.dumps(json_data, indent=2)
            except json.JSONDecodeError as json_err:
                text = raw_content
                logging.warning(f"JSON parsing error: {str(json_err)}. Using raw content.")
                
            return text
        except Exception as e:
            logging.error(f"Error extracting JSON text: {str(e)}")
            return None

    @staticmethod
    def save_extracted_markdown(key, extracted_text, file_type, subdir_name, save_to_local, bucket_name, destination_bucket):
        """Save extracted data as Markdown file locally or to S3"""
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
                
            logging.info(f"Saved markdown extraction for {key}")
            print(f"Extracted and saved as markdown: {key}")
            
        except Exception as e:
            logging.error(f"Error saving markdown for {key}: {str(e)}")
            print(f"Error saving markdown for {key}: {str(e)}")

    @staticmethod
    def get_safe_filename(key):
        """Extract a safe filename from the key"""
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