"""
Resume failed PDF extractions from progress files.
Retries failed files and saves them to a new retry folder.
"""

import os
import json
import boto3
from typing import List, Dict, Optional
from pathlib import Path
import time
import logging
from dataclasses import dataclass
from datetime import datetime
import click

from pdf_extract_nougat import NougatPDFExtractor, ProcessingResult, ProgressTracker

LOG_FILE = "resume_extraction.log"

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
class FailedFile:
    """Represents a failed file from the progress report."""
    s3_key: str
    error_message: str
    attempts: int
    server_used: Optional[str] = None


class ResumePDFExtractor:
    """
    Handles resuming failed PDF extractions from progress files.
    Retries failed files and saves them to a retry folder.
    
    Files with extracted text < 50 characters are NOT saved and marked as errors.
    Only generates a retry report - no progress tracking files are saved.
    The retry report is saved both locally and to S3 analytics folder.
    """
    
    def __init__(
        self,
        progress_file_path: str,
        bucket: str = None,
        max_workers: int = 4,
        nougat_servers: List[str] = None,
        timeout: int = 900,
        max_retries: int = 3,
        retry_destination: str = None
    ):
        self.progress_file_path = progress_file_path
        self.max_workers = max_workers
        self.timeout = timeout
        self.max_retries = max_retries
        
        # Load progress data
        self.progress_data = self._load_progress_file()
        self.failed_files = self._extract_failed_files()
        
        # Extract bucket info from progress or use provided
        self.bucket = bucket or self._infer_bucket_from_progress()
        self.original_destination = self._infer_destination_from_progress()
        
        # Set retry destination - use provided path or default to original + "_retry"
        if retry_destination:
            self.retry_destination = retry_destination.rstrip('/')
        else:
            self.retry_destination = f"{self.original_destination.rstrip('/')}_retry"
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )
        
        # Verify S3 connection
        self._verify_s3_connection()
        
        # Set up Nougat servers
        default_servers = [
            "http://127.0.0.1:8002/predict/",
            "http://127.0.0.1:8003/predict/",
            "http://127.0.0.1:8004/predict/"
        ]
        self.pdf_servers = nougat_servers if nougat_servers else default_servers
        logger.info(f"Using Nougat servers: {self.pdf_servers}")
        logger.info(f"Will retry {len(self.failed_files)} failed files")
        logger.info(f"Original destination: {self.original_destination}")
        logger.info(f"Retry destination: {self.retry_destination}")
    
    def _load_progress_file(self) -> dict:
        """Load and validate the progress file."""
        try:
            if not os.path.exists(self.progress_file_path):
                raise FileNotFoundError(f"Progress file not found: {self.progress_file_path}")
            
            with open(self.progress_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Loaded progress file: {self.progress_file_path}")
            logger.info(f"Progress status: {data.get('status', 'unknown')}")
            
            # Validate required fields
            required_fields = ['processed', 'pending', 'failed']
            for field in required_fields:
                if field not in data:
                    raise ValueError(f"Invalid progress file: missing '{field}' field")
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to load progress file: {str(e)}")
            raise
    
    def _extract_failed_files(self) -> List[FailedFile]:
        """Extract failed files from progress data."""
        failed_files = []
        for failed_entry in self.progress_data.get('failed', []):
            if isinstance(failed_entry, dict) and 'file' in failed_entry:
                failed_file = FailedFile(
                    s3_key=failed_entry['file'],
                    error_message=failed_entry.get('error', 'Unknown error'),
                    attempts=failed_entry.get('attempts', 0),
                    server_used=failed_entry.get('server_used')
                )
                failed_files.append(failed_file)
        
        logger.info(f"Found {len(failed_files)} failed files to retry")
        return failed_files
    
    def _infer_bucket_from_progress(self) -> str:
        """Try to infer bucket from the progress data or use default."""
        # Look for bucket info in processed files' paths
        processed_files = self.progress_data.get('processed', [])
        if processed_files:
            # This is a best-effort approach since bucket isn't stored in progress
            logger.warning("Bucket not explicitly found in progress file, using default 'esa-satcom-s3'")
            return "esa-satcom-s3"  # Default bucket from original code
        
        return "esa-satcom-s3"  # Default fallback
    
    def _infer_destination_from_progress(self) -> str:
        """Infer destination from progress file name or use default."""
        # Extract from progress file name pattern: extraction_progress_{prefix_name}.json
        progress_filename = os.path.basename(self.progress_file_path)
        if progress_filename.startswith('extraction_progress_') and progress_filename.endswith('.json'):
            prefix_name = progress_filename[len('extraction_progress_'):-len('.json')]
            return prefix_name
        
        logger.warning("Could not infer destination from progress file name, using 'raw_data_dedup_extractions'")
        return "raw_data_dedup_extractions"
    
    def _verify_s3_connection(self) -> None:
        """Verify S3 connection and bucket access."""
        try:
            # Test S3 connection by listing bucket
            self.s3_client.head_bucket(Bucket=self.bucket)
            logger.info(f"S3 connection verified for bucket: {self.bucket}")
        except Exception as e:
            logger.error(f"S3 connection failed: {str(e)}")
            raise ConnectionError(f"Cannot connect to S3 bucket '{self.bucket}': {str(e)}")
    
    def retry_failed_extractions(self) -> None:
        """Main method to retry failed extractions."""
        if not self.failed_files:
            logger.info("No failed files to retry")
            return
        
        logger.info(f"Starting retry process for {len(self.failed_files)} failed files")
        logger.info(f"Files will be saved to: {self.retry_destination}")
        logger.info("Note: Only retry report will be generated, no progress tracking files will be saved")
        logger.info("Note: Files with extracted text < 50 characters will NOT be saved and marked as errors")
        
        # Create a custom extractor for retry with modified destination
        retry_extractor = NougatPDFExtractor(
            bucket=self.bucket,
            prefix="",  # We'll process specific files, not scan a prefix
            save_to_local=False,
            destination_bucket=self.retry_destination,
            max_workers=self.max_workers,
            nougat_servers=self.pdf_servers,
            timeout=self.timeout,
            max_retries=self.max_retries
        )
        
        # Create a mock progress tracker that does nothing
        class MockProgressTracker:
            def initialize(self, files): pass
            def mark_completed(self, key, result): pass
            def finalize(self): pass
        
        # Disable progress tracking and report generation in the retry extractor
        retry_extractor.progress_tracker = MockProgressTracker()
        
        # Override the file listing to only process our failed files
        failed_keys = [f.s3_key for f in self.failed_files]
        retry_extractor._list_pdf_files = lambda: failed_keys
        
        # Override methods to prevent saving progress and reports
        retry_extractor._generate_report = lambda: None
        
        # Override the save method to enforce 50-character minimum
        original_save_method = retry_extractor.save_extracted_markdown
        def custom_save_method(key: str, extracted_text: str, is_error: bool = False):
            if not extracted_text:
                # Handle completely empty extraction
                return {
                    'status': 'error',
                    'error': 'Empty extraction (0 chars, minimum 50 required)',
                    'filename': None
                }
            elif len(extracted_text) < 50:
                # Handle short extraction
                return {
                    'status': 'error',
                    'error': f'Text too short ({len(extracted_text)} chars, minimum 50 required)',
                    'filename': None
                }
            return original_save_method(key, extracted_text, is_error)
        
        retry_extractor.save_extracted_markdown = custom_save_method
        
        # Start processing
        try:
            retry_extractor.process_files()
            
            # Check for files that are still failing after retry
            still_failing = [r for r in retry_extractor.results if r.status == "error"]
            if still_failing:
                logger.warning(f"Still error: {len(still_failing)} files still have errors after retry")
                for failed_result in still_failing:
                    logger.warning(f"{failed_result.s3_key}: {failed_result.error_message}")
            
            self._generate_retry_report(retry_extractor.results)
            logger.info("Retry process completed successfully")
        except Exception as e:
            logger.error(f"Retry process failed: {str(e)}")
            raise
    
    def _generate_retry_report(self, retry_results: List[ProcessingResult]) -> None:
        """Generate a report specifically for the retry operation."""
        try:
            # Calculate retry metrics
            success_results = [r for r in retry_results if r.status == "success"]
            error_results = [r for r in retry_results if r.status == "error"]
            success_count = len(success_results)
            error_count = len(error_results)
            total_retried = len(retry_results)
            
            # Create original failure summary
            original_failures = {}
            for failed_file in self.failed_files:
                original_failures[failed_file.s3_key] = {
                    "original_error": failed_file.error_message,
                    "original_attempts": failed_file.attempts,
                    "original_server": failed_file.server_used
                }
            
            # Performance metrics
            processing_times = [r.processing_time_seconds for r in retry_results if hasattr(r, 'processing_time_seconds')]
            avg_time = sum(processing_times) / len(processing_times) if processing_times else 0
            total_chars = sum(r.characters_extracted for r in success_results)
            
            # Categorize errors
            short_text_errors = [r for r in error_results if 'too short' in str(r.error_message).lower()]
            other_errors = [r for r in error_results if 'too short' not in str(r.error_message).lower()]
            
            # Create retry report
            current_time = datetime.now()
            retry_report = {
                "metadata": {
                    "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "original_progress_file": self.progress_file_path,
                    "original_destination": self.original_destination,
                    "retry_destination": self.retry_destination,
                    "total_files_retried": total_retried,
                    "retry_success_count": success_count,
                    "retry_error_count": error_count,
                    "retry_success_rate": f"{(success_count / total_retried * 100):.1f}%" if total_retried > 0 else "0.0%"
                },
                "retry_stats": {
                    "total_characters_extracted": total_chars,
                    "average_processing_time_seconds": round(avg_time, 2),
                    "files_successfully_recovered": success_count,
                    "files_still_failing": error_count,
                    "files_rejected_too_short": len(short_text_errors),
                    "files_with_other_errors": len(other_errors)
                },
                "original_failures": original_failures,
                "retry_results": [
                    {
                        "file": r.s3_key,
                        "status": r.status,
                        "chars_extracted": r.characters_extracted,
                        "processing_time": r.processing_time_seconds,
                        "error": r.error_message,
                        "server_used": r.server_used,
                        "retries_used": r.retries
                    }
                    for r in retry_results
                ]
            }
            
            # Save retry report (named after original destination)
            original_destination_name = self.original_destination.split('/')[-1] if '/' in self.original_destination else self.original_destination
            retry_report_filename = f"retry_report_{original_destination_name}.json"
            with open(retry_report_filename, "w", encoding="utf-8") as f:
                json.dump(retry_report, f, indent=2)
            logger.info(f"Saved retry report locally to {retry_report_filename}")
            
            # Upload retry report to S3 (same analytics folder as original reports)
            s3_report_key = f"data_extracted/_analytics_/{self.original_destination}/{retry_report_filename}"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_report_key,
                Body=json.dumps(retry_report, indent=2).encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'retry-success-count': str(success_count),
                    'retry-error-count': str(error_count),
                    'retry-short-text-errors': str(len(short_text_errors)),
                    'retry-other-errors': str(len(other_errors)),
                    'original-failures': str(len(self.failed_files))
                }
            )
            logger.info(f"Uploaded retry report to s3://{self.bucket}/{s3_report_key}")
            
        except Exception as e:
            logger.error(f"Failed to generate retry report: {str(e)}")
            raise
    
    def get_summary(self) -> dict:
        """Get a summary of what will be retried."""
        return {
            "progress_file": self.progress_file_path,
            "bucket": self.bucket,
            "original_destination": self.original_destination,
            "retry_destination": self.retry_destination,
            "failed_files_count": len(self.failed_files),
            "failed_files": [
                {
                    "file": f.s3_key,
                    "error": f.error_message,
                    "attempts": f.attempts
                }
                for f in self.failed_files[:10]  # Show first 10 for brevity
            ]
        }


@click.command()
@click.option("--progress-file", required=True, help="Path to the extraction progress JSON file")
@click.option("--bucket", help="S3 bucket (inferred from progress if not provided)")
@click.option("--max-workers", default=4, help="Number of parallel threads")
@click.option("--timeout", default=900, help="Timeout for API calls in seconds")
@click.option("--max-retries", default=3, help="Maximum retries for failed API calls")
@click.option("--retry-destination", help="Full path where to save retry extractions (default: original_destination_retry)")
@click.option("--servers", multiple=True, help="Nougat server URLs (can specify multiple)")
@click.option("--dry-run", is_flag=True, help="Show what would be retried without actually processing")
def resume_extraction(progress_file, bucket, max_workers, timeout, max_retries, retry_destination, servers, dry_run):
    """
    Resume failed PDF extractions from a progress file.
    
    Files with extracted text < 50 characters are NOT saved and marked as errors.
    Only generates retry_report_{original_destination}.json - no progress tracking files.
    The retry report is saved locally and uploaded to the same S3 analytics folder as original reports.
    """
    try:
        # Initialize the resume extractor
        resume_extractor = ResumePDFExtractor(
            progress_file_path=progress_file,
            bucket=bucket,
            max_workers=max_workers,
            nougat_servers=list(servers) if servers else None,
            timeout=timeout,
            max_retries=max_retries,
            retry_destination=retry_destination
        )
        
        # Show summary
        summary = resume_extractor.get_summary()
        logger.info("=== RETRY SUMMARY ===")
        logger.info(f"Progress file: {summary['progress_file']}")
        logger.info(f"S3 bucket: {summary['bucket']}")
        logger.info(f"Original destination: {summary['original_destination']}")
        logger.info(f"Retry destination: {summary['retry_destination']}")
        logger.info(f"Failed files to retry: {summary['failed_files_count']}")
        
        if summary['failed_files_count'] == 0:
            logger.info("No failed files to retry. Exiting.")
            return
        
        if dry_run:
            logger.info("=== DRY RUN - Files that would be retried ===")
            for i, failed_file in enumerate(summary['failed_files'], 1):
                logger.info(f"{i}. {failed_file['file']}")
                logger.info(f"   Error: {failed_file['error']}")
                logger.info(f"   Previous attempts: {failed_file['attempts']}")
            if summary['failed_files_count'] > 10:
                logger.info(f"... and {summary['failed_files_count'] - 10} more files")
            logger.info("Use --dry-run=false to actually retry the extractions")
            return
        
        # Confirm before proceeding
        logger.info("=== STARTING RETRY PROCESS ===")
        
        # Start the retry process
        resume_extractor.retry_failed_extractions()
        
    except Exception as e:
        logger.error(f"Resume extraction failed: {str(e)}")
        raise


if __name__ == "__main__":
    resume_extraction()
