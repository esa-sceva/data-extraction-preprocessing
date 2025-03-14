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
- [Gemini](notebooks/09-gemini.ipynb)
- [Qwen](notebooks/10-qwen.ipynb)
- [Pdfminer](notebooks/11-pdfminer.ipynb)
- Mathpix - Annotations generated via the webpage


[Ground truth creation notebook](notebooks/generate_groundtruth.ipynb) - Pdfs split into images, and encoded images and passed to gpt4 to get ground truths.

[Evaluation script](src/evaluate_gt.py) - This script compares the ground truth vs extractions, extracts tables and formulas and compute a levenshtein distance over it and saves in a sheet.


### Evaluation Results

| Tool          | Text Levenshtein score | Formulas Levenshtein score | Tables Levenshtein score |
|---------------|------------------------|----------------------------|--------------------------|
| Mathpix       | 0.83002                | 0.540                      | 0.5269                   |
| Markitdown    | 0.80                   | 0                          | 0                        |
| Nougat        | 0.73                   | 0.55                       | 0.41                     |
| Docling       | 0.78                   | 0                          | 0.40                     |
| Pymupdf4llm   | 0.80                   | 0                          | 0.26                     |
| Pypdf2        | 0.83                   | 0                          | 0                        |
| Marker        | 0.79                   | 0.31                       | 0.418                    |
| Unstructured  | 0.78                   | 0                          | 0                        |
| Pdfminer      | 0.80                   | 0                          | 0                        |
| Mistral       | 0.845                  | 0.53                       | 0.54                     |
