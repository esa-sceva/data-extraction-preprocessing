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

class LocalStorageS3Upload:
    def __init__(self, base_dir='', sub_folder='', save_to_local=False):
        """Initialize LocalStorage with a base directory for reading files and for writing results"""

        self.base_dir = Path(base_dir)
        self.raw_data_dir = self.base_dir
        self.save_to_local = save_to_local
        self.destination_bucket = "raw_data_estimation"
        self.sub_folder = sub_folder
        
        if not self.save_to_local:
            self.client: boto3.session.Session.client = boto3.client(
                "s3",
                region_name=os.getenv("AWS_REGION"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
            )
            self.bucket_name: Final[str] = os.getenv("AWS_BUCKET_NAME", 'llm4eo-s3')

            print("saving to s3")
        
        else:
            print("saving files to local directory")
            os.makedirs(f"{self.destination_bucket}", exist_ok=True)
            
            # If subdirectory not provided, auto-detect later
            if self.sub_folder:
                self._setup_directories(self.sub_folder)

        self.cumulative_token_count_words = 0
        self.cumulative_token_count_chars = 0
        self.global_summary_path = f"{self.destination_bucket}/global_summary.parquet"

        self.log_file = "token_count.log"
        logging.basicConfig(filename=self.log_file, level=logging.INFO,
                            format='%(asctime)s - %(message)s')
        
        self.count = 0

        # Load cumulative counts from existing global summary if available
        self.load_global_summary()

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
                    self.cumulative_token_count_words = last_row.get('word_token_count', 0)
                    self.cumulative_token_count_chars = last_row.get('char_token_count', 0)
                    logging.info(f"Loaded existing global summary: {self.cumulative_token_count_words} words, {self.cumulative_token_count_chars} chars")
            elif not self.save_to_local:
                try:
                    response = self.client.get_object(
                        Bucket=self.bucket_name,
                        Key=self.global_summary_path
                    )
                    df = pd.read_parquet(io.BytesIO(response['Body'].read()))
                    if not df.empty:
                        last_row = df.iloc[-1]
                        self.cumulative_token_count_words = last_row.get('word_token_count', 0)
                        self.cumulative_token_count_chars = last_row.get('char_token_count', 0)
                        logging.info(f"Loaded existing global summary from S3: {self.cumulative_token_count_words} words, {self.cumulative_token_count_chars} chars")
                except self.client.exceptions.NoSuchKey:
                    logging.info("No existing global summary found in S3")
        except Exception as e:
            logging.error(f"Error loading global summary: {str(e)}")
            print(f"Error loading global summary: {str(e)}")

    def list_objects(self):
        """List all files in raw_data directory and process them"""
        try:
            # If no subfolder specified, discover and process all subdirectories
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
                # Process the specified directory
                self._process_directory(self.raw_data_dir, self.sub_folder)
            
            # After processing all objects, update the global summary data
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
                # If no subdirectories found, use base directory name
                subdirs = [self.base_dir.name]
                
            logging.info(f"Discovered subdirectories: {', '.join(subdirs)}")
        except Exception as e:
            logging.error(f"Error discovering subdirectories: {str(e)}")
            print(f"Error discovering subdirectories: {str(e)}")
        
        return subdirs

    def _process_directory(self, directory_path, subdir_name):
        """Process all files in a specific directory"""
        try:
            # Save the current subfolder for this processing run
            self.current_sub_folder = subdir_name
            
            file_list = list(directory_path.glob('**/*'))
            files = [f for f in file_list if f.is_file()]
            
            for file_path in tqdm(files, desc=f"Processing files in {subdir_name}"):
                rel_path = file_path.relative_to(self.base_dir)
                logging.info(f"Found raw data object: {rel_path}")
                self.process_object(file_path)
            
        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")
            logging.error(f"Error processing directory {directory_path}: {str(e)}")

    def process_object(self, file_path):
        """Process a single file, extract tokens and save immediately"""
        try:
            file_extension = file_path.suffix.lower().lstrip('.')
            
            if file_extension == "pdf":
                self.process_pdf(file_path)
            elif file_extension == "html":
                self.process_html(file_path)
            else:
                logging.info(f"Unsupported file type: {file_extension}")
                
        except Exception as e:
            print(f"Error processing object {file_path}: {str(e)}")
            logging.error(f"Error processing object {file_path}: {str(e)}")

    def process_pdf(self, file_path):
        try:
            key = str(file_path.relative_to(self.base_dir))
            
            with open(file_path, 'rb') as file:
                reader = PdfReader(file)
                text = ""
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text
            
            # Get tokenized words
            words, word_token_count = self.count_words(text)
            
            # Get tokenized characters
            chars, char_token_count = self.count_characters(text)

            logging.info(f"{key} : Word Tokens = {word_token_count}, Character Tokens = {char_token_count}")

            print(f"PDF Name: {key}")
            print(f"Word Tokens: {word_token_count}")
            print(f"Character Tokens: {char_token_count}")
            
            self.cumulative_token_count_words += word_token_count
            self.cumulative_token_count_chars += char_token_count

            # Save file summary
            self.save_file_summary(key, 'pdf', word_token_count, char_token_count)
            
            # Save tokens for this file
            self.save_file_tokens(key, words, chars)

            logging.info(f"Cumulative word token count: {self.cumulative_token_count_words}")
            logging.info(f"Cumulative character token count: {self.cumulative_token_count_chars}")

        except Exception as e:
            print(f"Error processing PDF: {str(e)}")
            logging.error(f"Error processing PDF: {str(e)}")

    def process_html(self, file_path):
        try:
            key = str(file_path.relative_to(self.base_dir))
            
            with open(file_path, 'r', encoding='utf-8') as file:
                soup = BeautifulSoup(file, 'html.parser')
                text = soup.get_text()
                text = text.strip()
                text = os.linesep.join([s for s in text.splitlines() if s.strip()])
            
            # Get tokenized words
            words, word_token_count = self.count_words(text)
            
            # Get tokenized characters
            chars, char_token_count = self.count_characters(text)

            logging.info(f"{key} : Word Tokens = {word_token_count}, Character Tokens = {char_token_count}")

            print(f"HTML Name: {key}")
            print(f"Word Tokens: {word_token_count}")
            print(f"Character Tokens: {char_token_count}")

            self.cumulative_token_count_words += word_token_count
            self.cumulative_token_count_chars += char_token_count

            # Save file summary
            self.save_file_summary(key, 'html', word_token_count, char_token_count)
            
            # Save tokens for this file
            self.save_file_tokens(key, words, chars)

            logging.info(f"Cumulative word token count: {self.cumulative_token_count_words}")
            logging.info(f"Cumulative character token count: {self.cumulative_token_count_chars}")

        except Exception as e:
            print(f"Error processing HTML: {str(e)}")
            logging.error(f"Error processing HTML: {str(e)}")

    def save_file_summary(self, key, file_type, word_token_count, char_token_count):
        """Save summary information for a single file to S3"""
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Extract safe filename
            base_filename = self.get_safe_filename(key)
            
            # Create DataFrame for file summary
            file_summary_df = pd.DataFrame([{
                'filename': key,
                'file_type': file_type,
                'word_token_count': word_token_count,
                'char_token_count': char_token_count,
                'process_timestamp': timestamp
            }])
            
            # Use the current subfolder being processed
            current_subfolder = getattr(self, 'current_sub_folder', self.sub_folder)
            
            # Upload file summary
            file_summary_buffer = io.BytesIO()
            file_summary_df.to_parquet(file_summary_buffer)
            file_summary_buffer.seek(0)
            
            file_summary_key = f"{self.destination_bucket}/{current_subfolder}/summaries/{base_filename}_{timestamp}.parquet"

            if self.save_to_local:
                file_summary_df.to_parquet(f'{file_summary_key}')
            
            else:
                self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=file_summary_key,
                    Body=file_summary_buffer.getvalue()
                )
            
            logging.info(f"Uploaded file summary to {file_summary_key}")
            
        except Exception as e:
            logging.error(f"Error saving file summary for {key}: {str(e)}")
            print(f"Error saving file summary for {key}: {str(e)}")
    
    def save_file_tokens(self, key, words, chars):
        """Save tokens for a single file to S3"""
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Extract safe filename
            base_filename = self.get_safe_filename(key)
            
            # Use the current subfolder being processed
            current_subfolder = getattr(self, 'current_sub_folder', self.sub_folder)
            
            # Save word tokens
            word_tokens_list = [{'index': idx, 'token': token} for idx, token in enumerate(words)]
            if word_tokens_list:
                word_tokens_df = pd.DataFrame(word_tokens_list)
                
                word_tokens_buffer = io.BytesIO()
                word_tokens_df.to_parquet(word_tokens_buffer)
                word_tokens_buffer.seek(0)
                
                word_tokens_key = f"{self.destination_bucket}/{current_subfolder}/tokens/{base_filename}_words_{timestamp}.parquet"

                if self.save_to_local:
                    word_tokens_df.to_parquet(f'{word_tokens_key}')
                
                else:
                    self.client.put_object(
                        Bucket=self.bucket_name,
                        Key=word_tokens_key,
                        Body=word_tokens_buffer.getvalue()
                    )
                logging.info(f"Uploaded word tokens for {base_filename}")
            
            char_tokens_list = [{'index': idx, 'token': token} for idx, token in enumerate(chars)]
            char_tokens_df = pd.DataFrame(char_tokens_list)
            
            char_tokens_buffer = io.BytesIO()
            char_tokens_df.to_parquet(char_tokens_buffer)
            char_tokens_buffer.seek(0)
            
            char_tokens_key = f"{self.destination_bucket}/{current_subfolder}/tokens/{base_filename}_chars_{timestamp}.parquet"

            if self.save_to_local:
                char_tokens_df.to_parquet(f'{char_tokens_key}')
            
            else:
                self.client.put_object(
                    Bucket=self.bucket_name,
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
            
            # Create new summary record
            new_summary = {
                'word_token_count': self.cumulative_token_count_words,
                'char_token_count': self.cumulative_token_count_chars,
                'process_timestamp': timestamp
            }
            
            # Read existing summary if it exists, or create new one
            if self.save_to_local and Path(self.global_summary_path).exists():
                try:
                    existing_df = pd.read_parquet(self.global_summary_path)
                    # Append new summary
                    updated_df = pd.concat([existing_df, pd.DataFrame([new_summary])], ignore_index=True)
                except:
                    # If corrupted or can't read, create new
                    updated_df = pd.DataFrame([new_summary])
            else:
                updated_df = pd.DataFrame([new_summary])
                
            if self.save_to_local:
                # Save updated summary locally
                updated_df.to_parquet(self.global_summary_path)
            else:
                # Save to S3
                summary_buffer = io.BytesIO()
                updated_df.to_parquet(summary_buffer)
                summary_buffer.seek(0)
                
                self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=self.global_summary_path,
                    Body=summary_buffer.getvalue()
                )
            
            print(f"Successfully updated global summary data: {self.cumulative_token_count_words} words, {self.cumulative_token_count_chars} chars")
            logging.info(f"Successfully updated global summary data: {self.cumulative_token_count_words} words, {self.cumulative_token_count_chars} chars")
            
        except Exception as e:
            logging.error(f"Error updating global summary: {str(e)}")
            print(f"Error updating global summary: {str(e)}")
    
    def get_safe_filename(self, key):
        """Extract a safe filename from the key"""
        # Remove path and extension
        base_name = os.path.basename(key)
        base_name = os.path.splitext(base_name)[0]
        
        # Replace any non-alphanumeric characters with underscores
        import re
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        
        return safe_name
    
    def count_words(self, text: str):
        """ Counts tokens by splitting text by space and returns the token count """
        tokens = text.split()  # Split text by spaces
        return tokens, len(tokens)

    def count_characters(self, text: str):
        """ Counts characters in the text excluding spaces and returns the count """
        text_without_spaces = text.replace(" ", "")  # Remove spaces
        return list(text_without_spaces), len(text_without_spaces)

if __name__ == '__main__':
    client = LocalStorageS3Upload(base_dir='../raw-text-tokenization/data', sub_folder = 'imperative_space', save_to_local = False)
    client.list_objects()