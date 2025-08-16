"""
Efficient PDF text extraction using Nougat API.
Maintains retries, load balancing, and result tracking.
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
import backoff
from requests.exceptions import RequestException
import logging
from dataclasses import dataclass
import hashlib
import re
import tempfile

LOG_FILE = "nougat_extraction.log"

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



class NougatPDFExtractor:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        save_to_local: bool = False,
        destination_bucket: str = "raw_data_dedup_extractions",
        max_workers: int = 4,
        nougat_servers: List[str] = None,
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
        self.results: List[ProcessingResult] = []
        self.progress_tracker = ProgressTracker(
            bucket=bucket,  # Use the same bucket as the extractor
            destination_prefix=destination_bucket  # Where extracted files go
        )

        
        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )

        # Set up Nougat servers (no health check)
        default_servers = [
            "http://127.0.0.1:8002/predict/",
            "http://127.0.0.1:8003/predict/",
            "http://127.0.0.1:8004/predict/"
        ]
        self.pdf_servers = nougat_servers if nougat_servers else default_servers
        logger.info(f"Using Nougat servers: {self.pdf_servers}")

    def _get_next_server(self) -> str:
        """Improved server selection with better load balancing."""
        # Track active server usage
        if not hasattr(self, '_server_usage'):
            self._server_usage = {server: 0 for server in self.pdf_servers}
        
        # Select the server with the least current usage
        selected_server = min(self._server_usage.items(), key=lambda x: x[1])[0]
        self._server_usage[selected_server] += 1
        return selected_server
    
    def _release_server(self, server: str) -> None:
        """Release a server after processing completes."""
        if hasattr(self, '_server_usage'):
            self._server_usage[server] = max(0, self._server_usage[server] - 1)
    
    def process_files(self) -> None:
        """Main processing method with real-time progress tracking and proper error handling."""
        try:
            pdf_keys = self._list_pdf_files()
            logger.info(f"Found {len(pdf_keys)} PDF files to process")
            self.progress_tracker.initialize(pdf_keys)
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._process_wrapper, key, self._get_next_server()): key 
                    for key in pdf_keys
                }
                
                for future in tqdm(as_completed(futures), total=len(futures), desc="Processing PDFs"):
                    key = futures[future]
                    try:
                        result = future.result()
                        if result is None:  # Handle None returns explicitly
                            failed_result = ProcessingResult(
                                s3_key=key,
                                status="error",
                                characters_extracted=0,
                                processing_time_seconds=0,
                                error_message="Processing returned None",
                                server_used="unknown",
                                retries=self.max_retries
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
                            server_used="unknown",
                            retries=self.max_retries
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
    
    def _process_wrapper(self, key: str, server: str) -> ProcessingResult:
        try:
            return self.process_pdf_from_s3(key, server)
        except Exception as e:
            logger.error(f"Error processing {key}: {str(e)}")
            return ProcessingResult(
                s3_key=key,
                status="error",
                error_message=str(e))
            
        except Exception as e:
            logger.error(f"Error processing {key} on {server}: {str(e)}")
            return None 

    def _list_pdf_files(self) -> List[str]:
        """List all PDF files in the S3 prefix."""
        pdf_keys = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)

            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.lower().endswith('.pdf'):
                        pdf_keys.append(key)
        except Exception as e:
            logger.error(f"Error listing S3 files: {str(e)}")
            raise
        
        return pdf_keys

    @backoff.on_exception(backoff.expo, RequestException, max_tries=3)
    def _call_nougat_api(self, file_path: Path, endpoint: str) -> str:
        """Call Nougat API with retry logic."""
        try:
            with open(file_path, "rb") as f:
                files = {'file': (file_path.name, f, 'application/pdf')}
                headers = {'accept': 'application/json'}
                
                logger.info(f"Posting {file_path} to {endpoint}")
                response = requests.post(
                    endpoint,
                    headers=headers,
                    files=files,
                    timeout=self.timeout
                )
                
                response.raise_for_status()
                return response.text
        except RequestException as e:
            logger.warning(f"API call to {endpoint} failed: {str(e)}")
            raise
    
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

    def process_pdf_from_s3(self, key: str, endpoint: str) -> ProcessingResult:
        """Process a single PDF file with guaranteed result fields."""
        result = ProcessingResult(
            s3_key=key,
            status="started",
            characters_extracted=0,
            processing_time_seconds=0,
            error_message=None,
            server_used=endpoint,
            markdown_filename=None,  # Explicitly initialize
            file_size_bytes=0,
            md5_hash=None,
            retries=0
        )
        
        try:
            # Get file metadata
            file_info = self.s3_client.head_object(Bucket=self.bucket, Key=key)
            result.file_size_bytes = file_info['ContentLength']
            
            # Download and process file
            with tempfile.NamedTemporaryFile(suffix=".pdf") as temp_file:
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
                        extracted_text = self._call_nougat_api(Path(temp_file.name), endpoint)
                        duration = time.time() - start_time
                        
                        if extracted_text and len(extracted_text) > 100:
                            upload_result = self.save_extracted_markdown(key, extracted_text)
                            if upload_result['status'] == 'success':
                                result.status = "success"
                                result.characters_extracted = len(extracted_text)
                                result.processing_time_seconds = duration
                                result.markdown_filename = upload_result['filename']  # Always set
                                logger.info(
                                f"Extracted {result.characters_extracted} characters from {key} "
                                f"(Size: {result.file_size_bytes} bytes, "
                                f"Time: {result.processing_time_seconds:.2f}s)"
                            )
                                result.retries = attempt
                                break
                            else:
                                raise ValueError(f"Upload failed: {upload_result.get('error')}")
                        else:
                            raise ValueError(f"Empty extraction (got {len(extracted_text)} chars)")
                            
                    except Exception as e:
                        if attempt == self.max_retries:
                            # Save error version if we got any text
                            if extracted_text:
                                error_upload = self.save_extracted_markdown(key, extracted_text, is_error=True)
                                result.markdown_filename = error_upload['filename']  # Set even on error
                                result.characters_extracted = len(extracted_text)
                            result.status = "error"
                            result.error_message = str(e)
                        else:
                            time.sleep(2 ** attempt)
            
            return result
            
        except Exception as e:
            result.status = "error"
            result.error_message = str(e)
            return result
        
        finally:
            self.results.append(result)
            return result
            try:
                os.remove(local_path)
            except OSError:
                pass
           


    def _generate_report(self) -> None:
        """Generate and save report both locally and to S3."""
        try:
            success_count = sum(1 for r in self.results if r.status == "success")
            error_count = len(self.results) - success_count
            total_chars = sum(r.characters_extracted for r in self.results if r.status == "success")
            avg_time = sum(r.processing_time_seconds for r in self.results) / len(self.results) if self.results else 0
            
            # Create safe filename from destination bucket
            safe_bucket_name = re.sub(r'[^\w\-]', '_', self.destination_bucket)
            report_filename = f"processing_report_{safe_bucket_name}.json"
            
            # Create report content
            report_content = json.dumps({
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "destination_bucket": self.destination_bucket,
                    "total_files": len(self.results),
                    "success_count": success_count,
                    "error_count": error_count,
                    "success_rate": f"{(success_count / len(self.results)) * 100:.1f}%" if self.results else "0%",
                },
                "processing_stats": {
                    "total_characters_extracted": total_chars,
                    "average_processing_time_seconds": avg_time,
                },
                "error_details": {
                    "unique_error_messages": list({r.error_message for r in self.results if r.error_message}),
                    "error_examples": [r.s3_key for r in self.results if r.status == "error"][:5]
                }
            }, indent=2)

            # Save locally
            with open(report_filename, "w", encoding="utf-8") as f:
                f.write(report_content)
            logger.info(f"Saved report locally to {report_filename}")

            # Save to S3
            s3_report_key = f"{self.destination_bucket}/analytics/{report_filename}"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_report_key,
                Body=report_content.encode('utf-8'),
                ContentType='application/json'
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
        self.output_path = output_path or f"extraction_progress_{prefix_name}.json"        
        
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
                "server_used": result["server_used"]
            })
        else:
            self.progress_data["failed"].append({
                "file": file_key,
                "error": result["error_message"],
                "attempts": result["retries"] + 1,
                "server_used": result["server_used"]
            })
        self._save()
        
    def finalize(self):
        """Mark completion and upload final reports to S3"""
        self.progress_data["status"] = "completed"
        self.progress_data["completion_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Save local JSON first
        self._save()
        
        # Upload to S3
        try:
            s3_key = f"data_extracted/analytics/{prefix_name}/{os.path.basename(self.output_path)}"
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
@click.option("--bucket", default="esa-satcom-s3", help="S3 bucket")
@click.option("--prefix", default="MS2/sample/pdfs/", help="S3 prefix to scan for PDFs")
@click.option("--save-to-local", is_flag=True, help="Save extracted Markdown locally")
@click.option("--destination-bucket", required=True, help="Destination bucket/folder")
@click.option("--max-workers", default=4, help="Number of parallel threads")
@click.option("--timeout", default=300, help="Timeout for API calls in seconds")
@click.option("--max-retries", default=3, help="Maximum retries for failed API calls")
@click.option("--servers", multiple=True, help="Nougat server URLs (can specify multiple)")
def run_pipeline(bucket, prefix, save_to_local, destination_bucket, max_workers, timeout, max_retries, servers):
    """Run the Nougat PDF extraction pipeline."""
    extractor = NougatPDFExtractor(
        bucket=bucket,
        prefix=prefix,
        save_to_local=save_to_local,
        destination_bucket=destination_bucket,
        max_workers=max_workers,
        timeout=timeout,
        max_retries=max_retries,
        nougat_servers=list(servers) if servers else None
    )
    extractor.process_files()

if __name__ == "__main__":
    run_pipeline()