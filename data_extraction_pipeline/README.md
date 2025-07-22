## Data Extraction pipeline

This pipeline consists of tranforming PDFs contained in a S3 bucket/prefix or in local into a MarkDown text (.md or .mmd). To do that you need to:

### 1. Start the fastapi server

To start the fastapi server, after changing the port number in `app.py` in one terminal, run

```
python app.py --port <port_number>
```

You caan ope up to 4 workers at the same time, just be sure that some of the ports are not already opened, if so run to identify the PID:

```
ss -tulnp | grep ':<port_number_busy>'
```
then:

```
kill -9 <PID> 
```
to kill it.


### 2. Process PDFs with Nougat

Open another terminal, run the `pdf_extract_nougat.py` file after matching the port number of the fastapi endpoint.

```
python pdf_extract_nougat.py
```


#### Process pdf files using MARKER

1. Run the `pdf_extract_marker.py` file, this uses a sequential pipeline.
(Move the file from rerun_files.txt first using move.py script)

#### Process non-pdf

1. Run the `non_pdf_extract.py`, after adjusting the number of processes needed.
