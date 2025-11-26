# PII Removal Tools

Tools for detecting and removing Personally Identifiable Information (PII) using [GLiNER](https://huggingface.co/E3-JSI/gliner-multi-pii-domains-v1) and [Microsoft Presidio](https://github.com/microsoft/presidio) frameworks.

---

## Overview

This folder contains two main approaches for PII detection:
- **GLiNER**: Fast, GPU-optimized transformer model for entity recognition
- **Presidio**: Flexible NLP framework with multiple recognizer backends (Flair, spaCy, Transformers)

---

## Folder Structure

```
pii_removal/
├── gliner/                              # GLiNER-based approaches
│   ├── main_sentence_splits.py          # Simple sentence splitting (warning: truncates long sentences)
│   ├── main_tokenized.py                # DeBERTa tokenizer with offset calculation
│   ├── main_gliner_splitter.py          # Word splitter (poor performance, breaks sentences)
│   └── main_gliner_sentence_splitter.py # RECOMMENDED: Recursive sentence splitting
│
├── presidio/                            # Presidio framework
│   ├── __init__.py                      # Backwards compatibility exports
│   ├── helpers.py                       # Main helper functions
│   ├── nlp_engine_config.py             # NLP engine configurations
│   └── recognizers/
│       ├── __init__.py
│       └── flair_recognizer.py          # Custom Flair recognizer
│
└── tests/
    └── flair_test.py                    # Test script for Presidio Flair
```

---

## GLiNER Approaches

The folder contains 4 different text splitting strategies. **Use `main_gliner_sentence_splitter.py`** for production - the others have limitations:

- **`main_sentence_splits.py`** - Simple but truncates long sentences (not recommended)
- **`main_tokenized.py`** - Works for moderate documents but no sentence awareness
- **`main_gliner_splitter.py`** - Poor performance, breaks sentence structure (not recommended)
- **`main_gliner_sentence_splitter.py`** - **RECOMMENDED** (see below)

---

## Using GLiNER (Recommended Approach)

### Installation

```bash
pip install gliner nltk torch tqdm
python -c "import nltk; nltk.download('punkt')"
```

### How It Works

The recommended script (`main_gliner_sentence_splitter.py`):
1. **Splits text into sentences** using NLTK
2. **Checks token count** - if a sentence exceeds max_len, recursively splits it at punctuation marks or midpoint
3. **Processes in batches** with GPU memory management
4. **Maintains accurate offsets** for entity positions in the original text
5. **Replaces entities** with labeled placeholders like `[NAME: John Doe]`

### Basic Usage

Edit the script configuration:

```python
# Input/Output paths (edit in script)
input_dir = '/path/to/input/files'
output_dir = '/path/to/output/files'

# Model configuration
model = GLiNER.from_pretrained("E3-JSI/gliner-multi-pii-domains-v1").to('cuda')
labels = ["name", "organizations", "phone number", "email", "email address"]

# Processing parameters
max_len = 384          # Maximum tokens per chunk
batch_size = 4         # Adjust based on GPU memory
threshold = 0.5        # Entity confidence threshold (0.0-1.0)
```

Run the script:

```bash
python gliner/main_gliner_sentence_splitter.py
```

### Example Output

**Input:**
```
My name is John Doe and my email is john.doe@example.com. 
I work at OpenAI and can be reached at 555-1234.
```

**Output:**
```
My name is [NAME: John Doe] and my email is [EMAIL: john.doe@example.com]. 
I work at [ORGANIZATIONS: OpenAI] and can be reached at [PHONE NUMBER: 555-1234].
```

### Programmatic Usage

```python
from gliner import GLiNER
import torch

# Load model
device = 'cuda' if torch.cuda.is_available() else 'cpu'
model = GLiNER.from_pretrained("E3-JSI/gliner-multi-pii-domains-v1").to(device)

# Define entity types to detect
labels = ["name", "organizations", "phone number", "email", "email address"]

# Process long text (handles documents of any length)
from gliner.main_gliner_sentence_splitter import get_entities_from_long_text, replace_entities_with_labels

text = "Your long document text here..."
entities = get_entities_from_long_text(model, text, labels, max_len=384, batch_size=4)

# Redact PII
redacted_text = replace_entities_with_labels(text, entities)
print(redacted_text)
```

### Advanced Configuration

**Adjust sensitivity:**
```python
threshold = 0.3   # Lower = more sensitive (may include false positives)
threshold = 0.7   # Higher = more precise (may miss some entities)
```

**Optimize for GPU memory:**
```python
batch_size = 2    # If you get CUDA out of memory errors
batch_size = 8    # If you have plenty of GPU memory
```

**Handle very long documents:**
```python
max_len = 256     # Smaller chunks = faster processing
max_len = 512     # Larger chunks = better context but slower
```

---

## Presidio Framework

### Installation

```bash
pip install presidio-analyzer presidio-anonymizer
pip install flair spacy stanza transformers  # Choose your NLP backend
```

### Basic Usage

```python
from presidio.helpers import analyzer_engine, analyze, anonymize

# Initialize analyzer with Flair
analyzer = analyzer_engine(
    model_family="flair",
    model_path="flair/ner-english-large"
)

# Analyze text
results = analyze(
    model_family="flair",
    model_path="flair/ner-english-large",
    text="My name is John Doe and my email is john@example.com",
    entities=None,  # Auto-detect all supported entities
    language="en",
    score_threshold=0.35
)

# Anonymize
anonymized = anonymize(
    text=original_text,
    operator="replace",  # or "mask", "encrypt", "highlight"
    analyze_results=results
)
```

### Supported NLP Engines

| Engine | Model Example | Installation |
|--------|---------------|--------------|
| **Flair** | `flair/ner-english-large` | `pip install flair` |
| **spaCy** | `en_core_web_lg` | `pip install spacy && python -m spacy download en_core_web_lg` |
| **Stanza** | `en` | `pip install stanza` |
| **Transformers** | `StanfordAIMI/stanford-deidentifier-base` | `pip install transformers` |
| **Azure AI Language** | Azure endpoint | Azure subscription |

### Anonymization Operators

- **replace**: Replace with entity type label `[PERSON]`
- **mask**: Replace with masking characters `****`
- **encrypt**: Encrypt the entity (reversible)
- **highlight**: Mark entities for visualization

---

## Quick Comparison

| Feature | GLiNER | Presidio |
|---------|--------|----------|
| **Speed** | Very fast (GPU optimized) | Moderate (depends on backend) |
| **Accuracy** | High for supported entities | Varies by NLP engine |
| **Customization** | Limited to model entities | Highly customizable |
| **Entity Types** | Pre-trained set | Extensible with custom recognizers |
| **Dependencies** | Minimal (GLiNER only) | Multiple backends available |
| **Best For** | High-throughput processing | Flexible, custom PII detection |

---

## Entity Types

### GLiNER Default Labels
```python
["name", "organizations", "phone number", "email", "email address"]
```

### Presidio Supported Entities
- PERSON
- LOCATION
- ORGANIZATION
- PHONE_NUMBER
- EMAIL_ADDRESS
- DATE_TIME
- CREDIT_CARD
- IP_ADDRESS
- IBAN_CODE
- And more...


---

## Running Tests

```bash
# Test Presidio Flair recognizer
python tests/flair_test.py
```

---

## References

- **GLiNER**: [E3-JSI/gliner-multi-pii-domains-v1](https://huggingface.co/E3-JSI/gliner-multi-pii-domains-v1)
- **Presidio**: [Microsoft Presidio](https://github.com/microsoft/presidio)
- **Flair**: [Flair NLP](https://github.com/flairNLP/flair)
