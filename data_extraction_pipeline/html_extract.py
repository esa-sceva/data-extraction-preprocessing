"""
Efficient HTML text extraction using multiple processing methods.
Maintains retries, load balancing, and result tracking similar to PDF extraction.
"""

import os
import boto3
from typing import List, Dict, Optional
from pathlib import Path
from tqdm.auto import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import json
import logging
from dataclasses import dataclass
import hashlib
import re
import tempfile
from datetime import datetime
from trafilatura import extract as trafilatura_extract
from bs4 import BeautifulSoup, Comment
import html2text

LOG_FILE = "html_extraction.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),  # Log to file
        logging.StreamHandler()        # Log to console
    ]
)

logger = logging.getLogger(__name__)

@dataclass
class ProcessingResult:
    s3_key: str
    status: str
    characters_extracted: int = 0
    processing_time_seconds: float = 0.0
    error_message: Optional[str] = None
    server_used: Optional[str] = None
    markdown_filename: Optional[str] = None
    file_size_bytes: Optional[int] = None
    md5_hash: Optional[str] = None
    retries: int = 0
    html_processor: Optional[str] = None


class HTMLExtractor:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        save_to_local: bool = False,
        destination_bucket: str = "raw_data_dedup_extractions",
        max_workers: int = 4,
        html_processor: str = "trafilatura",
        timeout: int = 300,
        max_retries: int = 3
    ):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        self.save_to_local = save_to_local
        self.destination_bucket = destination_bucket
        self.max_workers = max_workers
        self.timeout = timeout
        self.max_retries = max_retries
        self.html_processor = html_processor
        self.results: List[ProcessingResult] = []
        
        self.progress_tracker = ProgressTracker(
            bucket=bucket,
            destination_prefix=destination_bucket
        )

        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )

        logger.info(f"Initialized HTMLExtractor using {html_processor} processor")
        
        # Initialize html2text converter
        self.h2t = html2text.HTML2Text()
        self.h2t.ignore_links = False
        self.h2t.ignore_images = False
        self.h2t.ignore_emphasis = False
        self.h2t.body_width = 0
        self.h2t.unicode_snob = True
        self.h2t.decode_errors = 'ignore'

    def process_files(self) -> None:
        """Main processing method with real-time progress tracking and proper error handling."""
        try:
            html_keys = self._list_html_files()
            logger.info(f"Found {len(html_keys)} HTML files to process")
            self.progress_tracker.initialize(html_keys)
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._process_wrapper, key): key 
                    for key in html_keys
                }
                
                for future in tqdm(as_completed(futures), total=len(futures), desc="Processing HTML files"):
                    key = futures[future]
                    try:
                        result = future.result()
                        if result is None:
                            failed_result = ProcessingResult(
                                s3_key=key,
                                status="error",
                                characters_extracted=0,
                                processing_time_seconds=0,
                                error_message="Processing returned None",
                                server_used=self.html_processor,
                                retries=self.max_retries,
                                html_processor=self.html_processor
                            )
                            self.results.append(failed_result)
                            self.progress_tracker.mark_completed(key, failed_result.__dict__)
                        else:
                            self.progress_tracker.mark_completed(key, result.__dict__)
                    except Exception as e:
                        logger.error(f"Failed to process {key}: {str(e)}")
                        failed_result = ProcessingResult(
                            s3_key=key,
                            status="error",
                            characters_extracted=0,
                            processing_time_seconds=0,
                            error_message=str(e),
                            server_used=self.html_processor,
                            retries=self.max_retries,
                            html_processor=self.html_processor
                        )
                        self.results.append(failed_result)
                        self.progress_tracker.mark_completed(key, failed_result.__dict__)
            
            self.progress_tracker.finalize()
            self._generate_report()
            
        except Exception as e:
            logger.error(f"Fatal error in process_files: {str(e)}")
            if hasattr(self, 'progress_tracker'):
                self.progress_tracker.progress_data["status"] = f"failed: {str(e)}"
                self.progress_tracker._save()
            raise

    def _process_wrapper(self, key: str) -> ProcessingResult:
        try:
            return self.process_html_from_s3(key)
        except Exception as e:
            logger.error(f"Error processing {key}: {str(e)}")
            return ProcessingResult(
                s3_key=key,
                status="error",
                error_message=str(e),
                html_processor=self.html_processor,
                server_used=self.html_processor
            )

    def _list_html_files(self) -> List[str]:
        """List all HTML files in the S3 prefix."""
        html_keys = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)

            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.lower().endswith(('.html', '.htm', '.xhtml')):
                        html_keys.append(key)
        except Exception as e:
            logger.error(f"Error listing S3 files: {str(e)}")
            raise
        
        return html_keys

    def _extract_html_content(self, file_path: Path) -> str:
        """Extract content from HTML file using specified processor."""
        encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings_to_try:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    html_content = f.read()
                
                if self.html_processor == "trafilatura":
                    return trafilatura_extract(html_content) or ""
                
                elif self.html_processor == "beautifulsoup":
                    return self._extract_with_beautifulsoup(html_content)
                
                elif self.html_processor == "html2text":
                    return self.h2t.handle(html_content)
                
                elif self.html_processor == "combined":
                    return self._extract_combined(html_content)
                
                else:
                    # Default to trafilatura
                    return trafilatura_extract(html_content) or ""
                    
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.warning(f"Error with {encoding} encoding: {str(e)}")
                continue
        
        raise ValueError("Failed to decode HTML file with any encoding")

    def _extract_with_beautifulsoup(self, html_content: str) -> str:
        """Extract content using BeautifulSoup."""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style", "nav", "header", "footer", "aside"]):
            script.decompose()
        
        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        # Get text content
        text = soup.get_text()
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text

    def _extract_combined(self, html_content: str) -> str:
        """Extract content using multiple methods and combine results."""
        results = []
        
        # Try trafilatura first
        traf_result = trafilatura_extract(html_content)
        if traf_result:
            results.append(f"## Trafilatura Extraction\n\n{traf_result}")
        
        # Try BeautifulSoup
        bs_result = self._extract_with_beautifulsoup(html_content)
        if bs_result and len(bs_result) > 100:
            results.append(f"## BeautifulSoup Extraction\n\n{bs_result}")
        
        return "\n\n---\n\n".join(results) if results else ""

    def save_extracted_markdown(self, key: str, extracted_text: str, is_error: bool = False) -> dict:
        """
        Save extracted markdown and return detailed status.
        
        Returns:
            dict: {
                'status': 'success'|'error',
                'filename': str,      # Final stored filename
                's3_key': str,        # Full S3 path (if uploaded)
                'local_path': str,    # Local path (if saved locally)
                'error': str          # Only present if failed
            }
        """
        result = {
            'status': 'success',
            'filename': None,
            's3_key': None,
            'local_path': None
        }
        
        try:
            if not extracted_text:
                raise ValueError("Empty extracted text")
                
            base_filename = self.get_safe_filename(key)
            if is_error:
                base_filename = f"error_{base_filename}"
            
            final_filename = f"{base_filename}.md"
            processed_text = self.process_text(extracted_text)
            
            if self.save_to_local:
                local_path = Path(self.destination_bucket) / final_filename
                local_path.parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, 'w', encoding='utf-8') as f:
                    f.write(processed_text)
                result.update({
                    'filename': final_filename,
                    'local_path': str(local_path)
                })
                logger.debug(f"Saved markdown locally: {local_path}")
            else:
                s3_key = f"{self.destination_bucket.strip('/')}/{final_filename}"
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    Body=processed_text.encode('utf-8'),
                    ContentType='text/markdown'
                )
                result.update({
                    'filename': final_filename,
                    's3_key': s3_key
                })
                logger.info(f"Uploaded markdown to S3: s3://{self.bucket}/{s3_key}")
                
            return result
            
        except Exception as e:
            error_msg = f"Failed to save {key}: {str(e)}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'error': error_msg,
                'filename': f"error_{self.get_safe_filename(key)}.md" if is_error else None
            }

    def process_html_from_s3(self, key: str) -> ProcessingResult:
        """Process a single HTML file with guaranteed result fields."""
        result = ProcessingResult(
            s3_key=key,
            status="started",
            characters_extracted=0,
            processing_time_seconds=0,
            error_message=None,
            server_used=self.html_processor,
            markdown_filename=None,
            file_size_bytes=0,
            md5_hash=None,
            retries=0,
            html_processor=self.html_processor
        )
        
        try:
            # Get file metadata
            file_info = self.s3_client.head_object(Bucket=self.bucket, Key=key)
            result.file_size_bytes = file_info['ContentLength']
            
            # Download and process file
            with tempfile.NamedTemporaryFile(suffix=".html") as temp_file:
                self.s3_client.download_fileobj(self.bucket, key, temp_file)
                temp_file.seek(0)
                
                # Calculate hash
                result.md5_hash = hashlib.md5(temp_file.read()).hexdigest()
                temp_file.seek(0)
                
                # Process with retries
                extracted_text = ""
                for attempt in range(self.max_retries + 1):
                    try:
                        start_time = time.time()
                        extracted_text = self._extract_html_content(Path(temp_file.name))
                        duration = time.time() - start_time
                        
                        if extracted_text and len(extracted_text) > 50:
                            upload_result = self.save_extracted_markdown(key, extracted_text)
                            if upload_result['status'] == 'success':
                                result.status = "success"
                                result.characters_extracted = len(extracted_text)
                                result.processing_time_seconds = duration
                                result.markdown_filename = upload_result['filename']
                                logger.info(
                                    f"Extracted {result.characters_extracted} characters from {key} "
                                    f"(Size: {result.file_size_bytes} bytes, "
                                    f"Time: {result.processing_time_seconds:.2f}s, "
                                    f"Processor: {self.html_processor})"
                                )
                                result.retries = attempt
                                break
                            else:
                                raise ValueError(f"Upload failed: {upload_result.get('error')}")
                        else:
                            raise ValueError(f"Empty extraction (got {len(extracted_text)} chars)")
                            
                    except Exception as e:
                        if attempt == self.max_retries:
                            if extracted_text:
                                error_upload = self.save_extracted_markdown(key, extracted_text, is_error=True)
                                result.markdown_filename = error_upload['filename']
                                result.characters_extracted = len(extracted_text)
                            result.status = "error"
                            result.error_message = str(e)
                        else:
                            time.sleep(2 ** attempt)
        
        except Exception as e:
            result.status = "error"
            result.error_message = str(e)
        
        finally:
            self.results.append(result)
            return result

    def _generate_report(self) -> None:
        """Generate and save report both locally and to S3 with standardized format."""
        try:
            # Calculate metrics
            success_results = [r for r in self.results if r.status == "success"]
            error_results = [r for r in self.results if r.status == "error"]
            success_count = len(success_results)
            error_count = len(error_results)
            total_files = len(self.results)
            
            # Processing stats
            total_chars = sum(r.characters_extracted for r in success_results)
            processing_times = [r.processing_time_seconds for r in self.results if hasattr(r, 'processing_time_seconds')]
            avg_time = sum(processing_times) / len(processing_times) if processing_times else 0
            
            # Performance metrics - calculate total time from progress tracker timestamps
            current_time = datetime.now()
            total_time_seconds = 0  
            total_time_minutes = 0
            files_per_min = 0
            
            # Use completion_time - timestamp from progress tracker for accurate total time
            try:
                if hasattr(self.progress_tracker, 'progress_data'):
                    start_time_str = self.progress_tracker.progress_data.get('timestamp')
                    end_time_str = self.progress_tracker.progress_data.get('completion_time')
                    
                    if start_time_str and end_time_str:
                        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
                        end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
                        total_time_seconds = (end_time - start_time).total_seconds()
                        total_time_minutes = total_time_seconds / 60
                        files_per_min = total_files / total_time_minutes if total_time_minutes > 0 else 0
                    else:
                        # Fallback to individual processing times if timestamps not available
                        if processing_times:
                            total_time_seconds = sum(processing_times)
                            total_time_minutes = total_time_seconds / 60
                            files_per_min = total_files / total_time_minutes if total_time_minutes > 0 else 0
                else:
                    # Fallback to individual processing times if progress tracker not available
                    if processing_times:
                        total_time_seconds = sum(processing_times)
                        total_time_minutes = total_time_seconds / 60
                        files_per_min = total_files / total_time_minutes if total_time_minutes > 0 else 0
            except Exception as e:
                logger.warning(f"Failed to calculate time from progress tracker: {str(e)}, using fallback")
                # Fallback to individual processing times
                if processing_times:
                    total_time_seconds = sum(processing_times)
                    total_time_minutes = total_time_seconds / 60
                    files_per_min = total_files / total_time_minutes if total_time_minutes > 0 else 0
            
            # Error details
            unique_errors = list({r.error_message for r in error_results if hasattr(r, 'error_message') and r.error_message})
            error_examples = [
                {"file": r.s3_key, "error": r.error_message, "processor": r.server_used} 
                for r in error_results 
                if hasattr(r, 's3_key') and hasattr(r, 'error_message') and hasattr(r, 'server_used')
            ] if error_results else []
    
            # Create report content with desired format
            report_content = {
                "metadata": {
                    "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "destination_bucket": self.destination_bucket,
                    "html_processor": self.html_processor,
                    "total_files": total_files,
                    "success_count": success_count,
                    "error_count": error_count,
                    "success_rate": f"{(success_count / total_files * 100):.1f}%" if total_files > 0 else "0.0%",
                },
                "processing_stats": {
                    "total_characters_extracted": total_chars,
                    "average_processing_time_seconds": round(avg_time, 2),
                    "html_processor_used": self.html_processor
                },
                "error_details": {
                    "unique_error_messages": unique_errors,
                    "error_examples": error_examples
                },
                "performance_metrics": {
                    "files_per_minute": round(files_per_min, 2),
                    "total_processing_time_minutes": round(total_time_minutes, 2),
                    "total_processing_time_seconds": round(total_time_seconds, 1)
                }
            }
    
            # Extract final prefix name from destination bucket
            prefix_name = self.destination_bucket.split('/')[-1] if self.destination_bucket else "unknown"
            report_filename = f"report_html_extraction_{prefix_name}.json"
            
            # Save locally
            with open(report_filename, "w", encoding="utf-8") as f:
                json.dump(report_content, f, indent=2)
            logger.info(f"Saved report locally to {report_filename}")
    
            # Save to S3
            s3_report_key = f"data_extracted/_analytics_/{prefix_name}/{report_filename}"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_report_key,
                Body=json.dumps(report_content, indent=2).encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'success-count': str(success_count),
                    'error-count': str(error_count),
                    'html-processor': self.html_processor
                }
            )
            logger.info(f"Uploaded report to s3://{self.bucket}/{s3_report_key}")
    
        except Exception as e:
            logger.error(f"Failed to generate report: {str(e)}")
            raise

    @staticmethod
    def process_text(text: str) -> str:
        """Clean and process the extracted text."""
        if not text:
            return ""
            
        text = text.strip('"')
        text = text.replace('\\n', '\n')
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    @staticmethod
    def get_safe_filename(key: str) -> str:
        """Create a safe filename from S3 key."""
        base_name = os.path.basename(key)
        base_name = os.path.splitext(base_name)[0]
        return re.sub(r'[^\w\-_]', '_', base_name)[:250]


class ProgressTracker:
    def __init__(
        self,
        bucket: str,
        destination_prefix: str,
        output_path: str = None
    ):
        # Extract the final part of the destination prefix for naming
        prefix_name = destination_prefix.rstrip("/").split("/")[-1]
        self.output_path = output_path or f"html_extraction_progress_{prefix_name}.json"        
        
        self.bucket = bucket
        self.destination_prefix = destination_prefix.rstrip("/") + "/"
        self.s3_client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )
        self.progress_data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "running",
            "processed": [],
            "pending": [],
            "failed": []
        }
        
    def initialize(self, all_files: List[str]):
        """Set up initial tracking state"""
        self.progress_data["pending"] = all_files.copy()
        self._save()
        
    def mark_completed(self, file_key: str, result: dict):
        """Update when a file finishes processing"""
        self.progress_data["pending"].remove(file_key)
        if result["status"] == "success":
            self.progress_data["processed"].append({
                "file": file_key,
                "markdown_file": result["markdown_filename"],
                "chars_extracted": result["characters_extracted"],
                "time_sec": result["processing_time_seconds"],
                "processor_used": result["server_used"],
                "html_processor": result.get("html_processor", "unknown")
            })
        else:
            self.progress_data["failed"].append({
                "file": file_key,
                "error": result["error_message"],
                "attempts": result["retries"] + 1,
                "processor_used": result["server_used"],
                "html_processor": result.get("html_processor", "unknown")
            })
        self._save()
        
    def finalize(self):
        """Mark completion and upload final reports to S3"""
        self.progress_data["status"] = "completed"
        self.progress_data["completion_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        prefix_name = self.destination_prefix.rstrip('/').split('/')[-1]

        # Save local JSON first
        self._save()
        
        # Upload to S3
        try:
            s3_key = f"data_extracted/_analytics_/{prefix_name}/{os.path.basename(self.output_path)}"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=json.dumps(self.progress_data, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
            logger.info(f"Uploaded progress report to s3://{self.bucket}/{s3_key}")
        except Exception as e:
            logger.error(f"Failed to upload progress report: {str(e)}")
            raise
        
    def _save(self):
        """Atomic save to local JSON"""
        temp_path = f"{self.output_path}.tmp"
        try:
            with open(temp_path, "w") as f:
                json.dump(self.progress_data, f, indent=2)
            os.replace(temp_path, self.output_path)
        except Exception as e:
            logger.error(f"Failed to save progress file: {str(e)}")
            raise


import click

@click.command()
@click.option("--bucket", default="", help="S3 bucket")
@click.option("--prefix", default="", help="S3 prefix to scan for HTML files")
@click.option("--save-to-local", is_flag=True, help="Save extracted Markdown locally")
@click.option("--destination-bucket", required=True, help="Destination bucket/folder")
@click.option("--max-workers", default=4, help="Number of parallel threads")
@click.option("--timeout", default=300, help="Timeout for operations in seconds")
@click.option("--max-retries", default=3, help="Maximum retries for failed operations")
@click.option("--html-processor", 
              type=click.Choice(['trafilatura', 'beautifulsoup', 'html2text', 'combined']),
              default='trafilatura', 
              help="HTML processing method")
def run_pipeline(bucket, prefix, save_to_local, destination_bucket, max_workers, timeout, max_retries, html_processor):
    """Run the HTML extraction pipeline with multiple processing options."""
    extractor = HTMLExtractor(
        bucket=bucket,
        prefix=prefix,
        save_to_local=save_to_local,
        destination_bucket=destination_bucket,
        max_workers=max_workers,
        html_processor=html_processor,
        timeout=timeout,
        max_retries=max_retries
    )
    extractor.process_files()

if __name__ == "__main__":
    run_pipeline()