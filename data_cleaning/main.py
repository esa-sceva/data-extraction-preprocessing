import argparse
import os
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Final, Optional

from tqdm.auto import tqdm

from helper.logger import Logger
from components.nougat_artifacts import NougatArtifactRemovalComponent
from storage.s3 import LocalStorageComponent, S3StorageComponent


class MarkdownCleaningPipeline:
    def __init__(self, base_dir: str, save_to_local: bool = True, num_processes: Optional[int] = None, debug: bool = False):
        self.base_dir = Path(base_dir)
        self.save_to_local = save_to_local
        self.num_processes = num_processes if num_processes else os.cpu_count()
        self.bucket_name: Final[str] = os.getenv("AWS_BUCKET_NAME", 'llm4eo-s3')
        self.destination_bucket: Final[str] = "cleaned_data"
        self.debug = debug
        
        self.logger = Logger("pipeline")
        if self.debug:
            self.logger.log("DEBUG mode enabled for pipeline.")
            
        self.components = [
            NougatArtifactRemovalComponent(debug=self.debug),
            # add other components here
        ]
        self.storage = LocalStorageComponent(self.destination_bucket) if save_to_local else S3StorageComponent(self.bucket_name, self.destination_bucket)

        if self.save_to_local:
            self.logger.log("Saving cleaned markdown files to local directory")
            os.makedirs(f"{self.destination_bucket}", exist_ok=True)
        else:
            self.logger.log("Saving cleaned markdown to S3")

    def process_files(self):
        try:
            markdown_files = []
            for root, _, files in os.walk(self.base_dir):
                for file in files:
                    if file.endswith('.md'):
                        markdown_files.append(Path(root) / file)
            
            self.logger.log(f"Found {len(markdown_files)} markdown files in {self.base_dir}")
            
            with Pool(processes = self.num_processes) as pool:
                process_func = partial(self.process_file_wrapper)
                list(tqdm(pool.imap(process_func, markdown_files),
                          total=len(markdown_files),
                          desc="Cleaning markdown files"))
        except Exception as e:
            self.logger.log(f"Error processing files: {str(e)}")

    def process_file_wrapper(self, file_path: Path):
        return self._process_file(file_path)

    def _process_file(self, file_path: Path) -> None:
        filename = file_path.name
        logger = Logger("cleaning")
        logger.log(f"[START] Cleaning {filename}")
        
        try:
            key = str(file_path.relative_to(self.base_dir))
            content = self.read_markdown_file(file_path)
            
            if content:
                for component in self.components:
                    cleaned_content = component.process(content, logger, filename)
                    if cleaned_content is None:
                        break
                if cleaned_content:
                    self.storage.save(key, cleaned_content, "", logger)
            else:
                logger.log(f"[ERROR] {filename} - Could not read markdown content")
        except Exception as e:
            logger.log(f"[ERROR] {filename} - Exception during processing: {str(e)}")

    def read_markdown_file(self, file_path: Path) -> Optional[str]:
        try:
            encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            for encoding in encodings_to_try:
                try:
                    with open(file_path, 'r', encoding=encoding) as file:
                        return file.read()
                except UnicodeDecodeError:
                    continue
            logger = Logger("read_errors")
            logger.log(f"[ERROR] {file_path.name} - Failed to decode with any encoding")
            return None
        except Exception as e:
            logger = Logger("read_errors")
            logger.log(f"[ERROR] {file_path.name} - Exception during reading: {str(e)}")
            return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Markdown Cleaning Pipeline")
    parser.add_argument("--base_dir", type=str, required=True, help="Base directory containing markdown files.")
    parser.add_argument("--save_to_s3", action="store_true", help="Save cleaned files to S3 instead of local. Default is local.")
    parser.add_argument("--num_processes", type=int, default=None, help="Number of processes to use. Defaults to CPU count.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging for components.")
    
    args = parser.parse_args()

    save_to_local_flag = not args.save_to_s3

    cleaner = MarkdownCleaningPipeline(
        base_dir=args.base_dir,
        save_to_local=save_to_local_flag,
        num_processes=args.num_processes,
        debug=args.debug
    )

    cleaner.process_files()