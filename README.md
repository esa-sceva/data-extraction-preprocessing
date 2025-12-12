# Data Extraction & Processing Pipeline

Complete end-to-end pipeline for extracting, cleaning, deduplicating, and removing PII from PDF and HTML documents. Designed for large-scale document processing with support for local and S3 storage.

---

## Overview

This repository provides a comprehensive data processing workflow:

1. **Extraction** - Convert PDFs/HTML to Markdown using Nougat and various HTML processors
2. **Deduplication** - Identify and remove near-duplicate documents using LSH algorithm
3. **Cleaning** - Remove OCR errors, artifacts, and formatting issues
4. **PII Removal** - Detect and redact personally identifiable information
5. **Analytics** - Analyze and verify processed data quality

**Key Features:**
- Multiprocessing support for high-performance batch processing
- S3 integration for cloud-based workflows
- Automatic retries with exponential backoff
- Comprehensive logging and progress tracking
- Modular, extensible architecture

---

## Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/esa-satcomllm/data-extraction.git
cd data-extraction

# Install dependencies for each component
cd data_extraction_pipeline && pip install -r requirements.txt
cd ../data_cleaning && pip install -r requirements.txt
cd ../pii_removal && pip install gliner nltk torch tqdm
cd ../duplication && pip install datasketch nltk tqdm
```

### AWS Configuration (if using S3)

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=eu-west-1
```

---

## Complete Workflow

### Step 1: Extract Documents to Markdown

Convert PDFs or HTML files to Markdown format.

#### PDF Extraction (Using Nougat)

```bash
cd data_extraction_pipeline

# Start Nougat API servers
python app.py --no-save --port 8002  # Terminal 1
python app.py --no-save --port 8003  # Terminal 2

# Extract PDFs from S3
python pdf_extract_nougat.py \
  --bucket esa-satcom-s3 \
  --prefix data/pdfs/ \
  --destination-bucket data_extracted/my_dataset \
  --max-workers 6 \
  --timeout 900
```

#### HTML Extraction

```bash
python html_extract.py \
  --bucket esa-satcom-s3 \
  --prefix data/html/ \
  --destination-bucket data_extracted/my_dataset \
  --html-processor trafilatura \
  --max-workers 8
```

**Output:** Markdown files in `data_extracted/my_dataset/`

> **Detailed docs:** [data_extraction_pipeline/README.md](data_extraction_pipeline/README.md)

---

### Step 2: Deduplicate Documents

Identify and remove near-duplicate documents using MinHash LSH.

```bash
cd ../duplication

# Edit lsh.py to configure paths
# FILE_DIR = '/path/to/extracted/markdown'
# THRESHOLD = 0.8  # 80% similarity threshold

python lsh.py
```

**Output:** `dupes.txt` containing groups of duplicate files

**What to do with duplicates:**
```python
# Example: Keep first file from each group, delete others
import json

with open('dupes.txt', 'r') as f:
    duplicates = eval(f.read())

for group in duplicates:
    keep = group[0]  # Keep first file
    remove = group[1:]  # Remove others
    print(f"Keep: {keep}")
    print(f"Remove: {', '.join(remove)}")
```


> **Detailed docs:** [duplication/README.md](duplication/README.md)

---

### Step 3: Clean Extracted Markdown

Remove OCR errors, Nougat artifacts, and formatting issues.

```bash
cd ../data_cleaning

# Clean markdown files (only the unique ones after deduplication)
python main.py \
  --base_dir /path/to/deduplicated/markdown \
  --num_processes 8 \
  --save_to_s3  # or save locally (default)
```

**Cleaning steps applied:**
1. OCR corrections (spacing between numbers/text)
2. Duplicate line removal
3. Nougat-specific formatting fixes
4. Rule-based text corrections
5. Artifact removal (warnings, error markers)

**Output:** Cleaned files in `raw_data_dedup_cleaned_v2/`

> **Detailed docs:** [data_cleaning/README.md](data_cleaning/README.md)

---

### Step 4: Remove PII (Personally Identifiable Information)

Detect and redact sensitive information using GLiNER or Presidio.

#### Using GLiNER (Recommended)

```bash
cd ../pii_removal

# Edit gliner/main_gliner_sentence_splitter.py to set paths
# input_dir = '/path/to/cleaned/markdown'
# output_dir = '/path/to/pii_removed/markdown'

python gliner/main_gliner_sentence_splitter.py
```

**Entities detected:**
- Names
- Organizations
- Phone numbers
- Email addresses
- And more...

**Output format:**
```
My name is [NAME: John Doe] and my email is [EMAIL: john@example.com].
```

#### Using Presidio (Alternative)

```python
from presidio.helpers import analyzer_engine, analyze

analyzer = analyzer_engine(model_family="flair", model_path="flair/ner-english-large")
results = analyze(text="...", language="en", score_threshold=0.35)
```

> **Detailed docs:** [pii_removal/README.md](pii_removal/README.md)

---

### Step 5: Analytics & Verification

Analyze processed data and verify quality.

#### Get Statistics

```bash
cd ../data_cleaning

# Calculate word/token counts
python scripts/stats.py --multi
```

**Output:**
- Total files processed
- Words and tokens per subfolder
- Average words/tokens per file

#### Compare Folders (S3)

```bash
cd ../analytics

# Compare extracted vs cleaned
python compare.py

# Sync missing files
python upload_missing.py

# Get character counts
python analytics.py
```

> **Detailed docs:** [analytics/README.md](analytics/README.md)

---

## Folder Structure

```
data-extraction/
├── data_extraction_pipeline/   # PDF/HTML → Markdown extraction
│   ├── app.py                   # Nougat FastAPI server
│   ├── pdf_extract_nougat.py    # PDF extraction script
│   ├── html_extract.py          # HTML extraction script
│   ├── resume.py                # Retry failed extractions
│   └── README.md
│
├── data_cleaning/               # Post-extraction cleaning
│   ├── main.py                  # Main cleaning pipeline
│   ├── components/              # Cleaning modules
│   ├── scripts/                 # Utility scripts (stats, etc.)
│   └── README.md
│
├── duplication/                 # Near-duplicate detection
│   ├── lsh.py                   # MinHash LSH implementation
│   └── README.md
│
├── pii_removal/                 # PII detection & removal
│   ├── gliner/                  # GLiNER-based approaches
│   ├── presidio/                # Presidio framework
│   ├── tests/
│   └── README.md
│
├── analytics/                   # Data analysis tools
│   ├── compare.py               # Compare folder structures
│   ├── analytics.py             # Character/file counting
│   ├── upload_missing.py        # Sync missing files
│   └── README.md
│
└── README.md                    # This file
```

---

## Common Workflows

### Workflow 1: Local Processing

```bash
# 1. Extract PDFs locally
cd data_extraction_pipeline
python pdf_extract_nougat.py \
  --bucket my-bucket \
  --prefix data/pdfs \
  --destination-bucket ./extracted_local \
  --save-to-local

# 2. Deduplicate first (before cleaning to save time)
cd ../duplication
# Edit lsh.py: FILE_DIR = '../data_extraction_pipeline/extracted_local'
python lsh.py
# Remove duplicates, keep only unique files

# 3. Clean unique files only
cd ../data_cleaning
python main.py \
  --base_dir ../duplication/unique_files \
  --num_processes 8

# 4. Remove PII
cd ../pii_removal
# Edit script paths
python gliner/main_gliner_sentence_splitter.py
```

---

### Workflow 2: S3-Based Processing

```bash
# 1. Extract from S3
cd data_extraction_pipeline
python pdf_extract_nougat.py \
  --bucket esa-satcom-s3 \
  --prefix data/arxiv \
  --destination-bucket data_extracted/arxiv \
  --max-workers 6

# 2. Download for deduplication
aws s3 sync s3://bucket/data_extracted/arxiv ./local_extracted

# 3. Deduplicate (process only unique files in next steps)
cd ../duplication
# Edit lsh.py: FILE_DIR = './local_extracted'
python lsh.py
# Remove duplicates based on dupes.txt

# 4. Clean only unique files
cd ../data_cleaning
python main.py \
  --base_dir ./unique_files \
  --save_to_s3 \
  --num_processes 8

# 5. PII removal and upload
cd ../pii_removal
python gliner/main_gliner_sentence_splitter.py
# Then: aws s3 sync ./output s3://bucket/data_final
```

---

### Workflow 3: Resume Failed Extractions

```bash
# Check progress file
cat extraction_progress_arxiv.json

# Retry failed files
cd data_extraction_pipeline
python resume.py \
  --progress-file extraction_progress_arxiv.json \
  --max-workers 3 \
  --retry-destination data_extracted/retries/arxiv_retry
```

---

## Performance Optimization

### Extraction
- **Parallel servers**: Run 3-4 Nougat servers on different ports
- **Worker tuning**: `--max-workers` = number_of_servers × 2
- **Timeout**: Increase for large PDFs (900-1200s)
- **Batch processing**: Use `split.py` for very large datasets

### Deduplication
- **Run early**: Deduplicate before cleaning to save processing time
- **Threshold tuning**: 
  - `0.6-0.7` for loose matching
  - `0.8-0.9` for strict matching
- **Batch size**: Increase `BATCH_SIZE` if you have more RAM

### Cleaning
- **Multiprocessing**: Adjust `--num_processes` based on CPU count
- **Debug mode**: Use `--debug` only for troubleshooting (slower)
- **I/O optimization**: Local SSD storage for better performance

### PII Removal
- **GPU memory**: Adjust `batch_size` (2-8) based on VRAM
- **Chunk size**: `max_len=384` for balance, `256` for speed, `512` for accuracy
- **Threshold**: `0.5` default, `0.3` for sensitivity, `0.7` for precision

---

## Output Files Summary

| Stage | Output Location | File Types |
|-------|----------------|------------|
| **Extraction** | `data_extracted/{prefix}/` | `.md` files, progress JSON, report JSON |
| **Deduplication** | Current directory | `dupes.txt` |
| **Cleaning** | `raw_data_dedup_cleaned_v2/` | `.md` files, logs |
| **PII Removal** | Configured output dir | `.md` files with PII redacted |
| **Analytics** | Current directory | `comparison_results.json`, stats |

---

## Troubleshooting

### Common Issues

**Port already in use (Nougat servers)**
```bash
# Find and kill process
netstat -ano | findstr :8002  # Windows
lsof -i :8002                  # Linux/Mac
kill -9 <PID>
```

**AWS credentials not found**
```bash
# Verify environment variables
echo $AWS_ACCESS_KEY_ID
# Or create .env file with credentials
```

**Out of memory during processing**
- Reduce `--num_processes` or `--max-workers`
- Process smaller batches
- Use cloud instances with more RAM

**CUDA out of memory (PII removal)**
```python
# Reduce batch size in script
batch_size = 2
max_len = 256
```

**Empty extractions from PDFs**
- Check if PDFs are scanned images (Nougat handles OCR)
- Increase timeout: `--timeout 1200`
- Review `error_` prefixed files in output

---

## Requirements

### Python Version
- Python 3.8+

### Key Dependencies
- `boto3` - AWS S3 operations
- `nougat-ocr` - PDF extraction
- `trafilatura`, `beautifulsoup4`, `html2text` - HTML parsing
- `nltk` - Text processing
- `gliner` - PII detection
- `presidio-analyzer` - Alternative PII framework
- `datasketch` - Deduplication
- `tqdm` - Progress bars

### Hardware Recommendations
- **CPU**: 8+ cores for parallel processing
- **RAM**: 16GB+ (32GB for large batches)
- **GPU**: NVIDIA GPU with 8GB+ VRAM (for PII removal)
- **Storage**: SSD recommended for I/O intensive operations

---

## Examples

### Example 1: Process Academic Papers

```bash
# Extract arXiv PDFs
python pdf_extract_nougat.py \
  --bucket research-papers \
  --prefix arxiv/2024 \
  --destination-bucket extracted/arxiv_2024 \
  --max-workers 6

# Deduplicate first (many papers have similar abstracts)
cd ../duplication
python lsh.py  # THRESHOLD=0.85 for academic text

# Clean only unique documents
cd ../data_cleaning
python main.py --base_dir ./unique_papers --num_processes 8

# Remove author names and institutions
cd ../pii_removal
python gliner/main_gliner_sentence_splitter.py
```

---

### Example 2: Process Legal Documents

```bash
# Extract
python pdf_extract_nougat.py --prefix legal_docs --destination-bucket legal_extracted

# Deduplicate
cd ../duplication
python lsh.py  # THRESHOLD=0.9 for legal docs

# Clean unique documents
cd ../data_cleaning
python main.py --base_dir ./unique_legal --save_to_s3

# Critical: Remove all PII with high sensitivity
cd ../pii_removal
# Edit script: threshold=0.3 for maximum detection
python gliner/main_gliner_sentence_splitter.py
```

---

### Example 3: Process Wikipedia HTML

```bash
# Extract HTML
python html_extract.py \
  --prefix data/wikipedia \
  --destination-bucket wiki_extracted \
  --html-processor trafilatura \
  --max-workers 12

# Deduplicate first
cd ../duplication && python lsh.py  # THRESHOLD=0.9

# Clean unique articles only
cd ../data_cleaning
python main.py --base_dir ./unique_wiki --num_processes 8

# Minimal PII in Wikipedia, but check
cd ../pii_removal
python gliner/main_gliner_sentence_splitter.py
```

---

## Best Practices

1. Always backup original data before processing
2. Test on small sample before full batch processing
3. Monitor logs for errors during extraction
4. Review duplicate groups before deleting files
5. Verify PII removal on sample files before deploying
6. Use version control for configuration changes
7. Document your workflow for reproducibility

---

## Project Status

**Production Ready:**
- PDF/HTML extraction
- Data cleaning pipeline
- Deduplication
- PII removal (GLiNER)

**Note:**
- PII removal not compatible with multiprocessing (run separately from cleaning)
- Presidio framework available but GLiNER recommended for performance

---

## Contributing

Contributions welcome! Each component has its own README with development guidelines:
- [Extraction Pipeline](data_extraction_pipeline/README.md)
- [Data Cleaning](data_cleaning/README.md)
- [Deduplication](duplication/README.md)
- [PII Removal](pii_removal/README.md)
- [Analytics](analytics/README.md)

---



## Support & Documentation

- **Extraction Issues**: See [data_extraction_pipeline/README.md](data_extraction_pipeline/README.md)
- **Cleaning Issues**: See [data_cleaning/README.md](data_cleaning/README.md)
- **PII Questions**: See [pii_removal/README.md](pii_removal/README.md)
- **Deduplication**: See [duplication/README.md](duplication/README.md)
- **Analytics**: See [analytics/README.md](analytics/README.md)

---

## License

This project is released under the Apache 2.0 License. See the [LICENSE](LICENSE) file for more details.

