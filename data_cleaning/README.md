# Data Cleaning Pipeline

Post-processing pipeline for cleaning extracted Markdown files from PDFs. Applies multiple cleaning components in sequence to remove OCR errors, Nougat artifacts, and other extraction issues.

---

## Overview

This pipeline processes Markdown files through a series of cleaning components:
1. **OCR Corrections** - Fixes spacing issues between numbers and text
2. **OCR Deduplication** - Removes duplicate lines from OCR errors
3. **Nougat Corrections** - Fixes Nougat-specific formatting issues
4. **Rule-Based Corrections** - Applies pattern-based fixes
5. **Nougat Artifact Removal** - Removes warning messages and error markers

**Features:**
- Multiprocessing support for high-performance batch processing
- Saves to local filesystem or S3
- Debug mode with before/after previews
- Comprehensive logging system
- Modular component-based architecture

---

## Installation

```bash
pip install -r requirements.txt
```

**Dependencies:**
- `boto3` - AWS S3 integration
- `tqdm` - Progress bars
- `nltk` - Text processing
- `RapidFuzz` - Fast fuzzy string matching
- `colorama` - Colored terminal output

---

## Quick Start

### Basic Usage (Local Output)

```bash
python main.py --base_dir /path/to/markdown/files --num_processes 8
```

### Save to S3

```bash
# Set AWS credentials
export AWS_BUCKET_NAME=your-bucket-name
export AWS_REGION=eu-west-1
export AWS_ACCESS_KEY=your-access-key
export AWS_SECRET_KEY=your-secret-key

# Run pipeline
python main.py --base_dir /path/to/markdown/files --save_to_s3 --num_processes 8
```

### Debug Mode

```bash
python main.py --base_dir /path/to/markdown/files --debug
```

Debug mode shows before/after content for each cleaning step with colored output.

---

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--base_dir` | Directory containing markdown files (recursive scan) | **Required** |
| `--save_to_s3` | Save to S3 instead of local directory | Local (flag) |
| `--num_processes` | Number of parallel processes | CPU count |
| `--debug` | Enable debug logging with previews | Disabled (flag) |

---

## Cleaning Components

### 1. OCR Corrections (`ocr_corrections.py`)

**Purpose:** Fixes spacing issues caused by OCR errors

**What it does:**
- Adds spaces between numbers and words (e.g., `123abc` → `123 abc`)
- Preserves common patterns like `20M`, `100k` (prevents breaking valid abbreviations)

**Example:**
```
Before: The study analyzed 1234samples from 56countries
After:  The study analyzed 1234 samples from 56 countries
```

---

### 2. OCR Duplicate Remover (`ocr_deduplication.py`)

**Purpose:** Removes duplicate consecutive lines from OCR errors

**What it does:**
- Detects and removes exact duplicate lines
- Uses fuzzy matching for near-duplicates
- Preserves intentional repetition

---

### 3. Nougat Corrections (`nougat_correction.py`)

**Purpose:** Fixes Nougat model-specific formatting issues

**What it does:**
- Corrects Nougat's special formatting patterns
- Fixes equation markers and mathematical notation
- Normalizes heading formats

---

### 4. Rule-Based Corrections (`rule_based_corrections.py`)

**Purpose:** Applies general text cleaning rules

**What it does:**
- Normalizes whitespace
- Fixes punctuation issues
- Removes excessive newlines
- Standardizes markdown formatting

---

### 5. Nougat Artifact Removal (`nougat_artifacts.py`)

**Purpose:** Removes Nougat extraction warnings and error markers

**What it does:**
- Removes `+++==WARNING: Truncated because of repetitions==...+++`
- Removes `+++==ERROR: No output for this page==...+++`
- Removes `[MISSING_PAGE_POST]` markers
- Strips escaped newlines (`\n` → actual newlines)
- Removes leading/trailing quotes

**Example:**
```
Before: "Text content\n+++==ERROR: No output for this page==+++\nMore text"
After:  Text content
        More text
```

---

## Output Structure

### Local Output

```
raw_data_dedup_cleaned_v2/
├── file1.md
├── file2.md
└── ...
```

Files are saved with sanitized filenames (special characters replaced with underscores).

### S3 Output

```
s3://your-bucket/raw_data_dedup_cleaned_v2/
├── file1.md
├── file2.md
└── ...
```

**Note:** The destination bucket/prefix is hardcoded as `raw_data_dedup_cleaned_v2` in `main.py` (line 26).

---

## Utility Scripts

### `scripts/stats.py` - Dataset Statistics

Calculate word and token counts across markdown files.

```bash
# Linear processing
python scripts/stats.py

# Multiprocessing mode
python scripts/stats.py --multi
```

**Output:**
- Files processed per subfolder
- Total words and tokens
- Average words/tokens per file
- Overall statistics

**Token estimation:** Uses `1.73` multiplier (words × 1.73 ≈ tokens)

---

### Other Scripts

| Script | Purpose |
|--------|---------|
| `s3_copy.py` | Copy files between S3 buckets |
| `sampling.py` | Random sampling of markdown files |
| `move_files.py` | Batch file operations |
| `push_logs_to_s3.py` | Upload logs to S3 |
| `latex_checker.py` | Check LaTeX syntax validity |
| `setup.sh` | Environment setup script |
| `setup_latex.sh` | LaTeX dependencies installer |

---


## Logging

Logs are written to component-specific log files:

- `pipeline.log` - Main pipeline events
- `cleaning.log` - Per-file cleaning operations
- `read_errors.log` - File reading errors
- `stats.log` - Statistics script output (if using `scripts/stats.py`)

**Log format:**
```
[START] Cleaning filename.md
[SUCCESS] filename.md - Fixed OCRCorrections
[SUCCESS] filename.md - Removed Nougat artifacts
[SAVE] File saved to local: output/filename.md
```

---

## Performance

### Multiprocessing

The pipeline uses Python's `multiprocessing.Pool` for parallel processing:

```python
# Adjust based on your CPU and I/O capacity
--num_processes 4   # Conservative (4 processes)
--num_processes 8   # Moderate (8 processes)
--num_processes 16  # Aggressive (16 processes)
```

**Recommendation:** Start with `CPU count - 1` and adjust based on performance.

### Encoding Support

Handles multiple encodings gracefully:
1. `utf-8` (primary)
2. `latin-1`
3. `cp1252`
4. `iso-8859-1`

Files that fail all encodings are logged and skipped.

---

## Folder Structure

```
data_cleaning/
├── main.py                      # Main pipeline script
├── requirements.txt             # Dependencies
├── README.md                    # This file
│
├── model/
│   └── base.py                  # Abstract base classes
│
├── components/                  # Cleaning components
│   ├── ocr_corrections.py
│   ├── ocr_deduplication.py
│   ├── nougat_correction.py
│   ├── rule_based_corrections.py
│   ├── nougat_artifacts.py
│   ├── nougat_helpers.py
│   ├── pii_remover.py           # (Not used - incompatible with multiprocessing)
│   ├── latex_artifacts.py
│   ├── presidio_helpers.py      # (For PII removal)
│   ├── presidio_nlp_engine_config.py
│   └── flair_recognizer.py
│
├── storage/
│   └── s3.py                    # Local and S3 storage components
│
├── helper/
│   └── logger.py                # Logging utility
│
└── scripts/                     # Utility scripts
    ├── stats.py                 # Dataset statistics
    ├── s3_copy.py
    ├── sampling.py
    ├── move_files.py
    ├── push_logs_to_s3.py
    ├── latex_checker.py
    ├── setup.sh
    └── setup_latex.sh
```

---

## Adding Custom Components

1. **Create your component:**

```python
# components/my_component.py
from model.base import DataProcessingComponent
from helper.logger import Logger
from typing import Optional

class MyComponent(DataProcessingComponent):
    def process(self, content: str, logger: Logger, filename: str) -> Optional[str]:
        try:
            # Your cleaning logic
            cleaned = content.replace("bad", "good")
            logger.log(f"[SUCCESS] {filename} - Applied MyComponent")
            return cleaned
        except Exception as e:
            logger.log(f"[ERROR] {filename} - MyComponent failed: {str(e)}")
            return content
```

2. **Add to pipeline:**

```python
# main.py
from components.my_component import MyComponent

self.components = [
    OCRCorrections(debug=self.debug),
    MyComponent(debug=self.debug),  # Add your component
    # ... other components
]
```

---

## Important Notes

### PII Removal

PII removal component (`pii_remover.py`) is **commented out** in the pipeline:

```python
# from components.pii_remover import PIIRemover
# PIIRemover(debug = self.debug),  # Not compatible with multiprocessing
```

**Reason:** PII models cannot be serialized for multiprocessing.

**Solution:** Run PII removal separately using the `../pii_removal/` tools.



---

## Troubleshooting

### Issue: Out of Memory

**Solution:** Reduce `--num_processes`:
```bash
python main.py --base_dir /path --num_processes 2
```

### Issue: Files Not Being Cleaned

**Solution:** Check logs for errors:
```bash
cat cleaning.log | grep ERROR
```

Enable debug mode to see content transformations:
```bash
python main.py --base_dir /path --debug
```

### Issue: S3 Upload Fails

**Solution:** Verify AWS credentials:
```bash
echo $AWS_ACCESS_KEY
echo $AWS_BUCKET_NAME
```

Check S3 bucket permissions (need `s3:PutObject`).

---

## Workflow Integration

This pipeline is typically used after PDF extraction:

```bash
# 1. Extract PDFs to Markdown
cd ../data_extraction_pipeline
python pdf_extract_nougat.py --bucket my-bucket --prefix data/pdfs

# 2. Clean the extracted Markdown
cd ../data_cleaning
python main.py --base_dir /path/to/extracted/markdown --num_processes 8

# 3. (Optional) Get statistics
python scripts/stats.py --multi

# 4. (Optional) Apply PII removal separately
cd ../pii_removal
python gliner/main_gliner_sentence_splitter.py
```

For more info, read: [README](https://github.com/esa-sceva/data-extraction/blob/main/README.md)

