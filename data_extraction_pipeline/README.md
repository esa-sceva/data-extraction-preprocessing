### How to run this pipeline


#### Process pdf files using NOUGAT

1. Start the fastapi server after changing the port number in `app.py` in one terminal.

```
python app.py
```

2. Open another terminal, run the `pdf_extract_nougat.py` file after matching the port number of the fastapi endpoint.


#### Process pdf files using MARKER

1. Run the `pdf_extract_marker.py` file, this uses a sequential pipeline.
(Move the file from rerun_files.txt first using move.py script)

#### Process non-pdf

1. Run the `non_pdf_extract.py`, after adjusting the number of processes needed.
