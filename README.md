## Eve-data-extraction

Data, notebooks and scripts used to experiment on different data extraction tools and evaluations.

Current tools experimented with

- [Markitdown](notebooks/01_markitdown.ipynb)
- [Nougat](notebooks/02_nougat.ipynb)
- [Docling](notebooks/03_docling.ipynb)
- [Pymupdf4llm](notebooks/04_pymupdf4llm.ipynb)
- [Pypdf2](notebooks/05_pypdf2.ipynb)
- [Unstructured.io](notebooks/06_unstructured.ipynb)
- [Mistral](notebooks/07-mistralocr.ipynb)
- [Marker](notebooks/08-marker.ipynb)
- [Gemini](notebooks/09-gemini.ipynb) --> triggers a content warning for many pdfs
- [Qwen](notebooks/10-qwen.ipynb) --> hallucinates with random tokens
- [Pdfminer](notebooks/11-pdfminer.ipynb)
- [GOT_OCR2](notebooks/12-GOT_OCR2_0.ipynb) --> hallucinates with random tokens
- [SmolDocling](notebooks/13-smoldocling.ipynb) --> seems to extract only half the page(max tokens 8192), raised an issue in the forum with the developer
- Mathpix - Annotations generated via the webpage


[Ground truth creation notebook](notebooks/generate_groundtruth.ipynb) - Pdfs split into images, and encoded images and passed to gpt4 to get ground truths.

[Evaluation script](src/evaluate_gt.py) - This script compares the ground truth vs extractions, extracts tables and formulas and compute a levenshtein ratio over it and saves in a sheet.


### Metric Used

Levenshtein ratio - This ratio takes the inverse of the levenshtein distance and normalizes it to a range between 0 to 1, higher the score, more the similarity.

### Latency tracked and hardware 

To track the latency per page, N = 5 runs of extraction from a large pdf of size 1011 pages was done for each tool, and then averaged to get the final result.
All experiments done via Colab.

### Evaluation Results

| Tool          | Text Levenshtein ratio | Formulas Levenshtein ratio | Tables Levenshtein ratio | Latency (per page) | Hardware  |
|---------------|------------------------|----------------------------|--------------------------|--------------------|-----------|
| Mathpix       | 0.84002                | 0.540                      | 0.5269                   | NA                 | NA        |
| Markitdown    | 0.81                   | 0                          | 0                        | 0.04 s             | Colab CPU |
| Nougat        | 0.74                   | 0.55                       | 0.41                     | 0.009 s            | Colab GPU |
| Docling       | 0.79                   | 0                          | 0.40                     | 0.42 s             | Colab GPU |
| Pymupdf4llm   | 0.81                   | 0                          | 0.26                     | 0.27 s             | Colab CPU |
| Pypdf2        | 0.84                   | 0                          | 0                        | 0.01 s             | Colab CPU |
| Marker        | 0.80                   | 0.31                       | 0.418                    | 0.22 s             | Colab GPU |
| Unstructured  | 0.79                   | 0                          | 0                        | 0.07 s             | Colab CPU |
| Pdfminer      | 0.81                   | 0                          | 0                        | 0.03 s             | Colab CPU |
| Mistral       | 0.855                  | 0.53                       | 0.54                     | NA                 | NA        |
