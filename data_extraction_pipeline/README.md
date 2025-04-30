### How to run this pipeline


#### Process pdf files using NOUGAT

1. Start the fastapi server after changing the port number in `app.py` in one terminal.

```
python app.py
```

2. Open another terminal, run the `pdf_extract_nougat.py` file after matching the port number of the fastapi endpoint.


#### Process non-pdf

1. Run the `non_pdf_extract.py`, after adjusting the number of processes needed.
