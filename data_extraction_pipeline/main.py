import os
from pathlib import Path
from tqdm import tqdm
from log_db import Severity, LogEntry
from file_manager import get_file_processor
from storage_manager import save_extracted_markdown
from utils import setup_directories, discover_subdirectories, get_files_in_directory

def process_file(file_path, subdir_name, save_to_local, bucket_name, destination_bucket):
    """Process a single file and extract its text content."""
    filename = file_path.name
    try:
        log_entry = LogEntry.start_new(filename, provider=subdir_name, 
                                       log_text=f'Started processing {file_path}...', 
                                       file_path=file_path)

        file_extension = file_path.suffix.lower().lstrip('.')

        key = str(file_path.relative_to(file_path.parent.parent)
                  if file_path.parent.parent != file_path.parent else file_path.name)

        processor_func, file_type = get_file_processor(file_extension)
        
        if processor_func is None or file_extension != 'pdf':
            log_entry.log(f"Unsupported file type: {file_extension}", severity=Severity.ERROR)
            log_entry.finalize_log("error")
            return

        extracted_text = processor_func(file_path, log_entry)

        if extracted_text:
            save_extracted_markdown(
                key, extracted_text, file_type, subdir_name, save_to_local, 
                bucket_name, destination_bucket, log_entry)
            text_len = len(extracted_text)
            log_entry.log(f"Extracted {text_len} characters.")
            log_entry.finalize_log("success", text_len)
        else:
            log_entry.log("Text extraction failed (empty result).", severity=Severity.ERROR)
            log_entry.finalize_log("error", 0)

    except Exception as e:
        log_entry.log(f"Error processing file: {str(e)}", severity=Severity.ERROR)
        log_entry.finalize_log("error")

class DataExtractionPipeline:
    """Main pipeline class for data extraction from files."""
    
    def __init__(self, base_dir='', sub_folder='', save_to_local=False):
        self.base_dir = Path(base_dir)
        self.save_to_local = save_to_local
        self.destination_bucket = "raw_data_dedup_extractions"
        self.sub_folder = sub_folder
        self.bucket_name = os.getenv("AWS_BUCKET_NAME", 'llm4eo-s3')
        
        if self.save_to_local:
            print("Saving files to local directory")
            setup_directories(self.destination_bucket, self.sub_folder)
        else:
            print("Saving to S3")

    def process_files(self):
        """Process all files in the specified directories."""
        try:
            if not self.sub_folder:
                subdirs = discover_subdirectories(self.base_dir)
                for subdir in subdirs:
                    print(f"Processing subdirectory: {subdir}")
                    if self.save_to_local:
                        setup_directories(self.destination_bucket, subdir)
                    subdir_path = self.base_dir / subdir
                    self._process_directory(subdir_path, subdir)
            else:
                self._process_directory(self.base_dir, self.sub_folder)

        except Exception as e:
            print(f"Error processing files: {str(e)}")

    def _process_directory(self, directory_path, subdir_name):
        """Process all files in a single directory."""
        try:
            files = get_files_in_directory(directory_path)
            print(f"Found {len(files)} files in {directory_path}")

            # Process files sequentially with progress bar
            for file in tqdm(files, desc=f"Processing files in {subdir_name}"):
                process_file(file, subdir_name, self.save_to_local, 
                             self.bucket_name, self.destination_bucket)

        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")

if __name__ == '__main__':
    extractor = DataExtractionPipeline(
        base_dir='data',
        save_to_local=True,
    )
    extractor.process_files()