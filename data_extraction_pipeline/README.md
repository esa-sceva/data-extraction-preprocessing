# Data Extraction Pipeline

This pipeline transforms PDFs and HTML files, stored either locally or in an S3 bucket, into Markdown files (.md or .mmd). It supports parallel processing, automatic retries, progress tracking, and comprehensive reporting.

## Features

- **PDF Extraction**: Uses Nougat API for high-quality PDF-to-Markdown conversion
- **HTML Extraction**: Multiple processors (Trafilatura, BeautifulSoup, html2text, or combined)
- **Parallel Processing**: Process multiple files concurrently with configurable workers
- **Progress Tracking**: Real-time progress monitoring with JSON reports
- **Retry Mechanism**: Automatic retries for failed extractions with exponential backoff
- **S3 Integration**: Direct S3 bucket operations for scalable processing
- **Analytics**: Detailed reports with performance metrics and error analysis

---

## Table of Contents

1. [Installation](#installation)
2. [AWS Configuration](#aws-configuration)
3. [Starting the Nougat API Server](#starting-the-nougat-api-server)
4. [PDF Extraction](#pdf-extraction)
5. [HTML Extraction](#html-extraction)
6. [Resuming Failed Extractions](#resuming-failed-extractions)
7. [Splitting Large Folders](#splitting-large-folders)
8. [Output Files](#output-files)
9. [Troubleshooting](#troubleshooting)

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/esa-satcomllm/data-extraction.git
cd data-extraction/data_extraction_pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

**Required packages include:**
- `boto3` - AWS S3 integration
- `requests` - API calls
- `nougat-ocr` - PDF processing
- `trafilatura`, `beautifulsoup4`, `html2text` - HTML processing
- `click` - CLI interface

---

## AWS Configuration

Set up your AWS credentials as environment variables. You have two options:

### Option 1: Export Environment Variables (Temporary)

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=eu-west-1
```

### Option 2: Use .env File (Recommended)

Create a `.env` file in the project directory:

```bash
nano .env
```

Add your credentials:

```
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=eu-west-1
```



---

## Starting the Nougat API Server

Before running PDF extraction, you need to start one or more FastAPI servers running the Nougat model.

### Start a Single Server

```bash
python app.py --no-save --port 8002
```

### Start Multiple Servers (for parallel processing)

Open separate terminal windows/sessions and run:

```bash
# Terminal 1
python app.py --no-save --port 8002

# Terminal 2
python app.py --no-save --port 8003

# Terminal 3
python app.py --no-save --port 8004
```

**Note:** Each server requires significant GPU/CPU resources. The default configuration uses ports 8002, 8003, and 8004.

### Custom Server Configuration

You can specify custom servers when running the extraction:

```bash
python pdf_extract_nougat.py --servers http://localhost:8005/predict/ --servers http://localhost:8006/predict/ ...
```

---

## PDF Extraction

Extract text from PDF files stored in S3 using the Nougat model.

### Basic Usage

```bash
python pdf_extract_nougat.py \
  --bucket esa-satcom-s3 \
  --prefix data/wikipedia \
  --destination-bucket data_extracted/wikipedia \
  --max-workers 6 \
  --timeout 900
```

### Parameters

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `--bucket` | S3 bucket name | - | No |
| `--prefix` | S3 prefix (folder path) to scan for PDFs | - | No |
| `--destination-bucket` | Destination folder/bucket for extracted .md files | - | **Yes** |
| `--max-workers` | Number of parallel processing threads | `4` | No |
| `--timeout` | API call timeout in seconds | `300` | No |
| `--max-retries` | Maximum retry attempts for failed files | `3` | No |
| `--save-to-local` | Save files locally instead of S3 | `True` | No |
| `--servers` | Custom Nougat server URLs (can specify multiple) | See above | No |

### Examples

**Process a specific folder:**
```bash
python pdf_extract_nougat.py \
  --bucket esa-satcom-s3 \
  --prefix data/mdpi_splitted/mdpi_27 \
  --destination-bucket data_extracted/mdpi_27 \
  --max-workers 6
```

**Save to local directory:**
```bash
python pdf_extract_nougat.py \
  --bucket esa-satcom-s3 \
  --prefix data/sample_pdfs \
  --destination-bucket ./local_extractions \
  --save-to-local \
  --max-workers 4
```

**With custom servers and extended timeout:**
```bash
python pdf_extract_nougat.py \
  --bucket esa-satcom-s3 \
  --prefix data/large_pdfs \
  --destination-bucket data_extracted/large_pdfs \
  --servers http://127.0.0.1:8002/predict/ \
  --servers http://127.0.0.1:8003/predict/ \
  --timeout 900 \
  --max-workers 2
```

### What Happens During Extraction?

1. **Scanning**: Lists all PDF files in the specified S3 prefix
2. **Processing**: Downloads each PDF, sends it to Nougat API servers
3. **Extraction**: Converts PDF to Markdown with automatic retries
4. **Storage**: Saves extracted .md files to S3 or local directory
5. **Tracking**: Creates progress file (`extraction_progress_{prefix}.json`)
6. **Reporting**: Generates analytics report (`report_extraction_{prefix}.json`)

---

## HTML Extraction

Extract text from HTML files using various processing methods.

### Basic Usage

```bash
python html_extract.py \
  --bucket esa-satcom-s3 \
  --prefix data/wikipedia \
  --destination-bucket data_extracted/wikipedia \
  --html-processor trafilatura
```

### Parameters

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `--bucket` | S3 bucket name | `esa-satcom-s3` | No |
| `--prefix` | S3 prefix to scan for HTML files | `MS2/sample/html/` | No |
| `--destination-bucket` | Destination folder for extracted .md files | - | **Yes** |
| `--max-workers` | Number of parallel threads | `4` | No |
| `--timeout` | Operation timeout in seconds | `300` | No |
| `--max-retries` | Maximum retry attempts | `3` | No |
| `--html-processor` | Processing method (see below) | `trafilatura` | No |
| `--save-to-local` | Save files locally instead of S3 | `False` | No |

### HTML Processors

Choose the best processor for your content:

| Processor | Description | Best For |
|-----------|-------------|----------|
| `trafilatura` | Fast, focused on main content | News articles, blogs, documentation |
| `beautifulsoup` | Clean HTML parsing | Well-structured HTML pages |
| `html2text` | Preserves links and structure | Technical docs, wikis |
| `combined` | Uses multiple methods | Complex pages, comprehensive extraction |

### Examples

**Extract Wikipedia pages:**
```bash
python html_extract.py \
  --bucket esa-satcom-s3 \
  --prefix data/wikipedia \
  --destination-bucket data_extracted/wikipedia \
  --html-processor trafilatura \
  --max-workers 8
```

**Use combined processing for better quality:**
```bash
python html_extract.py \
  --bucket esa-satcom-s3 \
  --prefix data/complex_html \
  --destination-bucket data_extracted/complex_html \
  --html-processor combined \
  --max-workers 4
```

**Save locally:**
```bash
python html_extract.py \
  --bucket esa-satcom-s3 \
  --prefix data/sample_html \
  --destination-bucket ./local_html_extractions \
  --html-processor beautifulsoup \
  --save-to-local
```

---

## Resuming Failed Extractions

If some files fail during extraction, you can retry them using the progress file.

### Basic Usage

```bash
python resume.py \
  --progress-file extraction_progress_wiley_1.json \
  --max-workers 3 \
  --retry-destination data_extracted/retries/wiley_1_retries \
  --bucket esa-satcom-s3
```

### Parameters

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `--progress-file` | Path to the progress JSON file | - | **Yes** |
| `--bucket` | S3 bucket (inferred if not provided) | Auto-detected | No |
| `--max-workers` | Number of parallel threads | `4` | No |
| `--timeout` | API call timeout in seconds | `900` | No |
| `--max-retries` | Maximum retry attempts | `3` | No |
| `--retry-destination` | Where to save retry extractions | `{original}_retry` | No |
| `--servers` | Nougat server URLs (multiple allowed) | Default servers | No |
| `--dry-run` | Show what would be retried without processing | `False` | No |

### Important Notes

- **Minimum Character Length**: Files with extracted text < 50 characters are **NOT saved** and marked as errors
- **No Progress Files**: Only generates `retry_report_{destination}.json` - no progress tracking files
- **S3 Upload**: The retry report is saved locally and uploaded to S3 analytics folder

### Examples

**Dry run to see what would be retried:**
```bash
python resume.py \
  --progress-file extraction_progress_wiley_1.json \
  --dry-run
```

**Retry with custom configuration:**
```bash
python resume.py \
  --progress-file extraction_progress_mdpi_27.json \
  --max-workers 3 \
  --retry-destination data_extracted/retries/mdpi_27_retries \
  --bucket esa-satcom-s3 \
  --timeout 1200
```

**Retry with specific servers:**
```bash
python resume.py \
  --progress-file extraction_progress_complex.json \
  --servers http://127.0.0.1:8002/predict/ \
  --servers http://127.0.0.1:8003/predict/ \
  --max-workers 2
```

---

## Splitting Large Folders

Split large S3 folders into smaller subfolders for easier management and processing.

### Basic Usage

```bash
python split.py \
  --bucket esa-satcom-s3 \
  --prefix data/mdpi/ \
  --fraction 0.04
```

### Parameters

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `--bucket` | S3 bucket name | - | **Yes** |
| `--prefix` | S3 prefix to split | - | **Yes** |
| `--fraction` | Fraction of files per subfolder | `0.05` | No |

### Example

Split a large folder into subfolders with ~4% of files each:

```bash
python split.py \
  --bucket esa-satcom-s3 \
  --prefix data/mdpi/ \
  --fraction 0.04
```

This creates subfolders like:
- `data/mdpi_splitted/mdpi_1/`
- `data/mdpi_splitted/mdpi_2/`
- `data/mdpi_splitted/mdpi_3/`
- etc.

---

## Output Files

The pipeline generates several types of files:

### 1. Extracted Markdown Files

**Location:** `{destination_bucket}/{safe_filename}.md`

Cleaned markdown text extracted from PDFs or HTML files.

### 2. Progress Files

**Location:** `extraction_progress_{prefix}.json` or `html_extraction_progress_{prefix}.json`

Real-time tracking of processing status:

```json
{
  "timestamp": "2025-11-26 10:30:00",
  "status": "running",
  "processed": [
    {
      "file": "data/file.pdf",
      "markdown_file": "file.md",
      "chars_extracted": 15420,
      "time_sec": 12.5,
      "server_used": "http://127.0.0.1:8002/predict/"
    }
  ],
  "pending": ["data/file2.pdf"],
  "failed": []
}
```

### 3. Extraction Reports

**Location:** 
- Local: `report_extraction_{prefix}.json` or `report_html_extraction_{prefix}.json`
- S3: `s3://{bucket}/data_extracted/_analytics_/{prefix}/report_*.json`

Comprehensive analytics:

```json
{
  "metadata": {
    "timestamp": "2025-11-26 11:00:00",
    "total_files": 100,
    "success_count": 95,
    "error_count": 5,
    "success_rate": "95.0%"
  },
  "processing_stats": {
    "total_characters_extracted": 1500000,
    "average_processing_time_seconds": 10.5
  },
  "performance_metrics": {
    "files_per_minute": 2.5,
    "total_processing_time_minutes": 40.0
  },
  "error_details": {
    "unique_error_messages": ["Timeout", "Empty extraction"],
    "error_examples": [...]
  }
}
```

### 4. Retry Reports

**Location:**
- Local: `retry_report_{destination}.json`
- S3: `s3://{bucket}/data_extracted/_analytics_/{original_destination}/retry_report_*.json`

Details of retry operations:

```json
{
  "metadata": {
    "timestamp": "2025-11-26 12:00:00",
    "original_destination": "data_extracted/mdpi_27",
    "retry_destination": "data_extracted/retries/mdpi_27_retries",
    "retry_success_count": 3,
    "retry_error_count": 2
  },
  "retry_stats": {
    "files_successfully_recovered": 3,
    "files_still_failing": 2,
    "files_rejected_too_short": 1
  }
}
```

### 5. Log Files

**Location:** Current directory

- `nougat_extraction.log` - PDF extraction logs
- `html_extraction.log` - HTML extraction logs
- `resume_extraction.log` - Retry operation logs

---

## Troubleshooting

### Common Issues

#### 1. Port Already in Use

**Error:** `Address already in use`

**Solution:**
```bash
# On Linux/Mac - Find process using port
lsof -i :8002
# Or
ss -tulnp | grep ':8002'

# Kill the process
kill -9 <PID>
```

**On Windows:**
```cmd
# Find process
netstat -ano | findstr :8002

# Kill process
taskkill /PID <PID> /F
```

#### 2. AWS Credentials Not Found

**Error:** `Unable to locate credentials`

**Solution:**
- Verify environment variables are set: `echo $AWS_ACCESS_KEY_ID`
- Check `.env` file exists and is properly formatted
- Ensure credentials have S3 read/write permissions

#### 3. Nougat Server Not Responding

**Error:** `Connection refused` or `Timeout`

**Solution:**
- Verify servers are running: `curl http://127.0.0.1:8002/`
- Check server logs for errors
- Increase `--timeout` value
- Reduce `--max-workers` to avoid overwhelming servers

#### 4. Out of Memory

**Error:** CUDA out of memory or system memory exhausted

**Solution:**
- Reduce `--max-workers`
- Process smaller batches using folder splitting
- Use fewer API servers
- Increase system/GPU memory

#### 5. Empty Extractions

**Issue:** Files have 0 or very few characters extracted

**Solution:**
- Check if PDFs are scanned images (Nougat handles OCR)
- Verify file integrity on S3
- For HTML: try different `--html-processor` options
- Review files marked as `error_` in output

#### 6. S3 Upload Failures

**Error:** `Access Denied` or upload errors

**Solution:**
- Verify AWS credentials have `s3:PutObject` permission
- Check bucket policy and CORS settings
- Ensure destination path doesn't conflict with existing files

---



## Support

For issues, questions, or contributions:
- Check the log files in the current directory
- Review error messages in the analytics reports
- Consult the progress JSON files for detailed processing status

---

## License


