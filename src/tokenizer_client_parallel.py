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

class LocalStorageS3Upload:
    def __init__(self, base_dir='', sub_folder='', save_to_local=False, num_processes=None):
        """Initialize LocalStorage with a base directory and multiprocessing support"""
        self.base_dir = Path(base_dir)
        self.raw_data_dir = self.base_dir
        self.save_to_local = save_to_local
        self.destination_bucket = "raw_data_estimation"
        self.sub_folder = sub_folder
        self.num_processes = num_processes if num_processes else os.cpu_count()

        print(self.num_processes)
        
        self.bucket_name: Final[str] = os.getenv("AWS_BUCKET_NAME", 'llm4eo-s3')
        if not self.save_to_local:
            print("saving to s3")
        else:
            print("saving files to local directory")
            os.makedirs(f"{self.destination_bucket}", exist_ok=True)
            if self.sub_folder:
                self._setup_directories(self.sub_folder)

        self.manager = Manager()
        self.cumulative_counts = self.manager.dict()
        self.cumulative_counts['words'] = 0
        self.cumulative_counts['chars'] = 0

        self.global_summary_path = f"{self.destination_bucket}/global_summary.parquet"
        self.log_file = "token_count.log"
        logging.basicConfig(filename=self.log_file, level=logging.INFO,
                          format='%(asctime)s - %(message)s')
        
        self.count = 0
        self.load_global_summary()

    def _get_s3_client(self):
        """Create a new S3 client instance"""
        return boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )

    def _setup_directories(self, sub_folder):
        """Setup necessary directories for a given subfolder"""
        os.makedirs(f"{self.destination_bucket}/{sub_folder}", exist_ok=True)
        os.makedirs(f"{self.destination_bucket}/{sub_folder}/tokens", exist_ok=True)
        os.makedirs(f"{self.destination_bucket}/{sub_folder}/summaries", exist_ok=True)

    def __str__(self):
        return f"LocalStorageS3Upload: {self.base_dir} -> {self.bucket_name if not self.save_to_local else self.destination_bucket}"

    def __repr__(self):
        return self.__str__()

    @property
    def total_files(self):
        """Count total number of files in raw_data directory"""
        self.count = 0
        try:
            for file in self.raw_data_dir.glob('**/*'):
                if file.is_file():
                    self.count += 1
        except Exception as e:
            logging.error(f"Error counting files: {str(e)}")
        return self.count

    def load_global_summary(self):
        """Load existing global summary if available"""
        try:
            if self.save_to_local and Path(self.global_summary_path).exists():
                df = pd.read_parquet(self.global_summary_path)
                if not df.empty:
                    last_row = df.iloc[-1]
                    self.cumulative_counts['words'] = last_row.get('word_token_count', 0)
                    self.cumulative_counts['chars'] = last_row.get('char_token_count', 0)
                    logging.info(f"Loaded existing global summary: {self.cumulative_counts['words']} words, {self.cumulative_counts['chars']} chars")
            elif not self.save_to_local:
                client = self._get_s3_client()
                try:
                    response = client.get_object(
                        Bucket=self.bucket_name,
                        Key=self.global_summary_path
                    )
                    df = pd.read_parquet(io.BytesIO(response['Body'].read()))
                    if not df.empty:
                        last_row = df.iloc[-1]
                        self.cumulative_counts['words'] = last_row.get('word_token_count', 0)
                        self.cumulative_counts['chars'] = last_row.get('char_token_count', 0)
                        logging.info(f"Loaded existing global summary from S3: {self.cumulative_counts['words']} words, {self.cumulative_counts['chars']} chars")
                except client.exceptions.NoSuchKey:
                    logging.info("No existing global summary found in S3")
        except Exception as e:
            logging.error(f"Error loading global summary: {str(e)}")
            print(f"Error loading global summary: {str(e)}")

    def list_objects(self):
        """List all files and process them using multiprocessing"""
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
                self._process_directory(self.raw_data_dir, self.sub_folder)
            
            self.update_global_summary()
            
        except Exception as e:
            print(f"Error listing objects: {str(e)}")
            logging.error(f"Error listing objects: {str(e)}")

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
            self.current_sub_folder = subdir_name
            file_list = list(directory_path.glob('**/*'))
            files = [f for f in file_list if f.is_file()]
            
            with Pool(processes=self.num_processes) as pool:
                process_func = partial(self.process_object_wrapper, 
                                     subdir_name=subdir_name,
                                     save_to_local=self.save_to_local,
                                     bucket_name=self.bucket_name,
                                     destination_bucket=self.destination_bucket)
                results = list(tqdm(pool.imap(process_func, files), 
                                  total=len(files), 
                                  desc=f"Processing files in {subdir_name}"))
            
            for word_count, char_count in results:
                if word_count is not None and char_count is not None:
                    with self.manager.Lock():
                        self.cumulative_counts['words'] += word_count
                        self.cumulative_counts['chars'] += char_count
            
        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")
            logging.error(f"Error processing directory {directory_path}: {str(e)}")

    @staticmethod
    def process_object_wrapper(file_path, subdir_name, save_to_local, bucket_name, destination_bucket):
        """Wrapper function for multiprocessing"""
        rel_path = file_path.relative_to(file_path.parent.parent)
        logging.info(f"Found raw data object: {rel_path}")
        return LocalStorageS3Upload.process_object_static(
            file_path, subdir_name, save_to_local, bucket_name, destination_bucket)

    @staticmethod
    def process_object_static(file_path, subdir_name, save_to_local, bucket_name, destination_bucket):
        """Static method to process a single file"""
        try:
            file_extension = file_path.suffix.lower().lstrip('.')
            key = str(file_path.relative_to(file_path.parent.parent))
            
            if file_extension == "pdf":
                return LocalStorageS3Upload.process_pdf_static(
                    file_path, key, subdir_name, save_to_local, bucket_name, destination_bucket)
            elif file_extension == "html":
                return LocalStorageS3Upload.process_html_static(
                    file_path, key, subdir_name, save_to_local, bucket_name, destination_bucket)
            elif file_extension == "txt":
                return LocalStorageS3Upload.process_txt_static(
                    file_path, key, subdir_name, save_to_local, bucket_name, destination_bucket)
            elif file_extension == "json":
                return LocalStorageS3Upload.process_json_static(
                    file_path, key, subdir_name, save_to_local, bucket_name, destination_bucket)
            else:
                logging.info(f"Unsupported file type: {file_extension}")
                return 0, 0
                
        except Exception as e:
            print(f"Error processing object {file_path}: {str(e)}")
            logging.error(f"Error processing object {file_path}: {str(e)}")
            return 0, 0

    @staticmethod
    def process_pdf_static(file_path, key, subdir_name, save_to_local, bucket_name, destination_bucket):
        """Static method to process PDF file"""
        try:
            with open(file_path, 'rb') as file:
                reader = PdfReader(file)
                text = "".join(page.extract_text() or "" for page in reader.pages)
            
            words, word_token_count = LocalStorageS3Upload.count_words(text)
            chars, char_token_count = LocalStorageS3Upload.count_characters(text)

            logging.info(f"{key} : Word Tokens = {word_token_count}, Character Tokens = {char_token_count}")
            print(f"PDF Name: {key}\nWord Tokens: {word_token_count}\nCharacter Tokens: {char_token_count}")
            
            LocalStorageS3Upload.save_file_summary_static(
                key, 'pdf', word_token_count, char_token_count, subdir_name, 
                save_to_local, bucket_name, destination_bucket)
            LocalStorageS3Upload.save_file_tokens_static(
                key, words, chars, subdir_name, save_to_local, bucket_name, destination_bucket)
            
            return word_token_count, char_token_count
            
        except Exception as e:
            print(f"Error processing PDF: {str(e)}")
            logging.error(f"Error processing PDF: {str(e)}")
            return 0, 0

    @staticmethod
    def process_html_static(file_path, key, subdir_name, save_to_local, bucket_name, destination_bucket):
        """Static method to process HTML file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                soup = BeautifulSoup(file, 'html.parser')
                text = soup.get_text().strip()
                text = os.linesep.join([s for s in text.splitlines() if s.strip()])
            
            words, word_token_count = LocalStorageS3Upload.count_words(text)
            chars, char_token_count = LocalStorageS3Upload.count_characters(text)

            logging.info(f"{key} : Word Tokens = {word_token_count}, Character Tokens = {char_token_count}")
            print(f"HTML Name: {key}\nWord Tokens: {word_token_count}\nCharacter Tokens: {char_token_count}")
            
            LocalStorageS3Upload.save_file_summary_static(
                key, 'html', word_token_count, char_token_count, subdir_name, 
                save_to_local, bucket_name, destination_bucket)
            LocalStorageS3Upload.save_file_tokens_static(
                key, words, chars, subdir_name, save_to_local, bucket_name, destination_bucket)
            
            return word_token_count, char_token_count
            
        except Exception as e:
            print(f"Error processing HTML: {str(e)}")
            logging.error(f"Error processing HTML: {str(e)}")
            return 0, 0
    
    @staticmethod
    def process_txt_static(file_path, key, subdir_name, save_to_local, bucket_name, destination_bucket):
        """Static method to process TXT file"""
        try:
            # Try different encodings in order
            encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            text = None
            
            for encoding in encodings_to_try:
                try:
                    with open(file_path, 'r', encoding=encoding) as file:
                        text = file.read()
                    # If we successfully read the file, break the loop
                    logging.info(f"Successfully read {key} with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    # If this encoding doesn't work, try the next one
                    continue
            
            # If none of the encodings worked
            if text is None:
                logging.error(f"Could not decode {key} with any of the attempted encodings")
                print(f"Error: Could not decode {key} with any of the attempted encodings")
                return 0, 0
                
            words, word_token_count = LocalStorageS3Upload.count_words(text)
            chars, char_token_count = LocalStorageS3Upload.count_characters(text)
            
            logging.info(f"{key} : Word Tokens = {word_token_count}, Character Tokens = {char_token_count}")
            print(f"TXT Name: {key}\nWord Tokens: {word_token_count}\nCharacter Tokens: {char_token_count}")
            
            LocalStorageS3Upload.save_file_summary_static(
                key, 'txt', word_token_count, char_token_count, subdir_name, 
                save_to_local, bucket_name, destination_bucket)
            LocalStorageS3Upload.save_file_tokens_static(
                key, words, chars, subdir_name, save_to_local, bucket_name, destination_bucket)
            
            return word_token_count, char_token_count
            
        except Exception as e:
            print(f"Error processing TXT: {str(e)}")
            logging.error(f"Error processing TXT: {str(e)}")
            return 0, 0

    @staticmethod
    def process_json_static(file_path, key, subdir_name, save_to_local, bucket_name, destination_bucket):
        """Static method to process JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                raw_content = file.read()
                
            try:
                json_data = json.loads(raw_content)
                text = json.dumps(json_data)
            except json.JSONDecodeError as json_err:
                # If JSON parsing fails, use the raw content and log the error
                text = raw_content
                logging.warning(f"JSON parsing error in {key}: {str(json_err)}. Using raw content for counting.")
                print(f"Warning: JSON parsing error in {key}: {str(json_err)}. Using raw content for counting.")
                
            words, word_token_count = LocalStorageS3Upload.count_words(text)
            chars, char_token_count = LocalStorageS3Upload.count_characters(text)
            
            logging.info(f"{key} : Word Tokens = {word_token_count}, Character Tokens = {char_token_count}")
            print(f"JSON Name: {key}\nWord Tokens: {word_token_count}\nCharacter Tokens: {char_token_count}")
            
            LocalStorageS3Upload.save_file_summary_static(
                key, 'json', word_token_count, char_token_count, subdir_name, 
                save_to_local, bucket_name, destination_bucket)
            LocalStorageS3Upload.save_file_tokens_static(
                key, words, chars, subdir_name, save_to_local, bucket_name, destination_bucket)
            
            return word_token_count, char_token_count
            
        except Exception as e:
            print(f"Error processing JSON: {str(e)}")
            logging.error(f"Error processing JSON: {str(e)}")
            return 0, 0

    @staticmethod
    def save_file_summary_static(key, file_type, word_token_count, char_token_count, subdir_name, 
                               save_to_local, bucket_name, destination_bucket):
        """Static method to save file summary"""
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = LocalStorageS3Upload.get_safe_filename_static(key)
            file_summary_df = pd.DataFrame([{
                'filename': key,
                'file_type': file_type,
                'word_token_count': word_token_count,
                'char_token_count': char_token_count,
                'process_timestamp': timestamp
            }])
            
            file_summary_buffer = io.BytesIO()
            file_summary_df.to_parquet(file_summary_buffer)
            file_summary_buffer.seek(0)
            
            file_summary_key = f"{destination_bucket}/{subdir_name}/summaries/{base_filename}.parquet"

            if save_to_local:
                file_summary_df.to_parquet(f'{file_summary_key}')
            else:
                client = boto3.client(
                    "s3",
                    region_name=os.getenv("AWS_REGION"),
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
                )
                client.put_object(
                    Bucket=bucket_name,
                    Key=file_summary_key,
                    Body=file_summary_buffer.getvalue()
                )
            logging.info(f"Uploaded file summary to {file_summary_key}")
            
        except Exception as e:
            logging.error(f"Error saving file summary for {key}: {str(e)}")
            print(f"Error saving file summary for {key}: {str(e)}")

    @staticmethod
    def save_file_tokens_static(key, words, chars, subdir_name, save_to_local, bucket_name, destination_bucket):
        """Static method to save file tokens"""
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = LocalStorageS3Upload.get_safe_filename_static(key)
            
            word_tokens_list = [{'index': idx, 'token': token} for idx, token in enumerate(words)]
            if word_tokens_list:
                word_tokens_df = pd.DataFrame(word_tokens_list)
                word_tokens_buffer = io.BytesIO()
                word_tokens_df.to_parquet(word_tokens_buffer)
                word_tokens_buffer.seek(0)
                
                word_tokens_key = f"{destination_bucket}/{subdir_name}/tokens/{base_filename}_words.parquet"

                if save_to_local:
                    word_tokens_df.to_parquet(f'{word_tokens_key}')
                else:
                    client = boto3.client(
                        "s3",
                        region_name=os.getenv("AWS_REGION"),
                        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                        aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
                    )
                    client.put_object(
                        Bucket=bucket_name,
                        Key=word_tokens_key,
                        Body=word_tokens_buffer.getvalue()
                    )
                logging.info(f"Uploaded word tokens for {base_filename}")
            
            char_tokens_list = [{'index': idx, 'token': token} for idx, token in enumerate(chars)]
            char_tokens_df = pd.DataFrame(char_tokens_list)
            char_tokens_buffer = io.BytesIO()
            char_tokens_df.to_parquet(char_tokens_buffer)
            char_tokens_buffer.seek(0)
            
            char_tokens_key = f"{destination_bucket}/{subdir_name}/tokens/{base_filename}_chars.parquet"

            if save_to_local:
                char_tokens_df.to_parquet(f'{char_tokens_key}')
            else:
                client = boto3.client(
                    "s3",
                    region_name=os.getenv("AWS_REGION"),
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
                )
                client.put_object(
                    Bucket=bucket_name,
                    Key=char_tokens_key,
                    Body=char_tokens_buffer.getvalue()
                )
            logging.info(f"Uploaded char tokens for {base_filename}")
            
        except Exception as e:
            logging.error(f"Error saving tokens for {key}: {str(e)}")
            print(f"Error saving tokens for {key}: {str(e)}")

    def update_global_summary(self):
        """Update the global summary file with current counts"""
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            new_summary = {
                'word_token_count': self.cumulative_counts['words'],
                'char_token_count': self.cumulative_counts['chars'],
                'process_timestamp': timestamp
            }
            
            if self.save_to_local and Path(self.global_summary_path).exists():
                try:
                    existing_df = pd.read_parquet(self.global_summary_path)
                    updated_df = pd.concat([existing_df, pd.DataFrame([new_summary])], ignore_index=True)
                except:
                    updated_df = pd.DataFrame([new_summary])
            else:
                updated_df = pd.DataFrame([new_summary])
                
            if self.save_to_local:
                updated_df.to_parquet(self.global_summary_path)
            else:
                client = self._get_s3_client()
                summary_buffer = io.BytesIO()
                updated_df.to_parquet(summary_buffer)
                summary_buffer.seek(0)
                client.put_object(
                    Bucket=self.bucket_name,
                    Key=self.global_summary_path,
                    Body=summary_buffer.getvalue()
                )
            
            print(f"Successfully updated global summary: {self.cumulative_counts['words']} words, {self.cumulative_counts['chars']} chars")
            logging.info(f"Successfully updated global summary: {self.cumulative_counts['words']} words, {self.cumulative_counts['chars']} chars")
            
        except Exception as e:
            logging.error(f"Error updating global summary: {str(e)}")
            print(f"Error updating global summary: {str(e)}")

    @staticmethod
    def get_safe_filename_static(key):
        """Extract a safe filename from the key"""
        base_name = os.path.basename(key)
        base_name = os.path.splitext(base_name)[0]
        import re
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        return safe_name

    @staticmethod
    def count_words(text: str):
        """Counts tokens by splitting text by space"""
        tokens = text.split()
        return tokens, len(tokens)

    @staticmethod
    def count_characters(text: str):
        """Counts characters excluding spaces"""
        text_without_spaces = text.replace(" ", "")
        return list(text_without_spaces), len(text_without_spaces)

if __name__ == '__main__':
    client = LocalStorageS3Upload(
        base_dir='../raw-text-tokenization/data',
        save_to_local=True,
    )
    client.list_objects()