## Data Extraction pipeline

This pipeline transforms PDFs, stored either locally or in an S3 bucket, into Markdown files (.md or .mmd).
Follow the steps below to set it up and run the extraction.


### 1. Start the FastAPI server

First, start the FastAPI server, which acts as the backend for PDF-to-Markdown extraction.

In a terminal, launch the server using:

```
python app.py --port <port_number>
```

You can start up to 4 workers in parallel using different ports.
If a port is already in use, find the process ID (PID) using:

```
ss -tulnp | grep ':<port_number_busy>'
```
Then terminate the process with:
```
kill -9 <PID> 
```


### 2. Process PDFs with Nougat

In a new terminal, run `pdf_extract_nougat.py`, ensuring that the ports match those used by the FastAPI workers.

```
python pdf_extract_nougat.py --bucket <bucket_name> --prefix <prefix/to/folders/> --num_workers 4 --save_to_local
```
NOTE: When using the --prefix option, all PDF files in the specified folder and its subfolders will be processed.

