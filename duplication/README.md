# Duplicate Detection

Fast near-duplicate document detection using MinHash LSH (Locality-Sensitive Hashing).

---

## Overview

`lsh.py` finds near-duplicate documents in a directory using the [datasketch](https://github.com/ekzhu/datasketch) library. It uses MinHash signatures and LSH indexing for efficient similarity detection across large document collections.

---

## How It Works

1. **Shingling**: Converts documents into character n-grams (shingles)
2. **MinHash**: Creates compact signatures representing each document
3. **LSH Indexing**: Groups similar documents using locality-sensitive hashing
4. **Duplicate Detection**: Identifies groups of near-duplicate files based on Jaccard similarity

---

## Usage

### Basic Example

```python
from lsh import LSH

# Initialize
lsh = LSH(
    FILE_DIR='data',           # Directory to scan
    SHINGLE_SIZE=3,            # N-gram size
    NUM_PERM=128,              # Hash permutations
    THRESHOLD=0.8,             # Similarity threshold (0-1)
    BATCH_SIZE=1000            # Memory management
)

# Find duplicates
duplicates = lsh.get_duplicates()

# Save results
with open("dupes.txt", 'w') as f:
    f.write(str(duplicates))
```

### Command Line

```bash
python lsh.py
```

Default configuration (edit in script):
```python
FILE_DIR = 'data'       # Folder to scan
SHINGLE_SIZE = 3        # N-gram size
NUM_PERM = 128          # Number of hash functions
THRESHOLD = 0.8         # 80% similarity threshold
BATCH_SIZE = 1000       # Batch size for processing
```

---

## Configuration Parameters

| Parameter | Description | Default | Impact |
|-----------|-------------|---------|--------|
| `SHINGLE_SIZE` | Size of n-grams (words) | `3` | Larger = more specific matching |
| `NUM_PERM` | Hash permutations | `128` | Higher = more accurate, more memory |
| `THRESHOLD` | Similarity threshold (0-1) | `0.8` | Higher = stricter matching (80%+) |
| `BATCH_SIZE` | Files per batch | `1000` | Adjust for available memory |

---

## Output

**Format:** Groups of near-duplicate files

```python
[
    ['data/file1.txt', 'data/file2.txt', 'data/file3.txt'],  # Group 1
    ['data/fileA.txt', 'data/fileB.txt']                      # Group 2
]
```

**Output file:** `dupes.txt` (list of duplicate groups)

---

## Installation

```bash
pip install datasketch nltk tqdm
```

---

## Use Cases

- **Data cleaning**: Remove duplicate documents before training
- **Quality control**: Identify redundant content in datasets
- **Deduplication**: Clean extracted data from web scraping
- **Content analysis**: Find similar documents in large corpora

---

## Performance

- **Scalability**: Efficiently handles millions of documents
- **Speed**: Sublinear query time using LSH
- **Memory**: Processes files in batches to manage memory usage

**Example:** ~1000 documents processed in seconds



---

## Reference

Based on [datasketch](https://github.com/ekzhu/datasketch) - MinHash, LSH, and probabilistic data structures

