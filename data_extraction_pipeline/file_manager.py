import os
from bs4 import BeautifulSoup
from pypdf import PdfReader
import json
from log_db import Severity

def extract_pdf_text(file_path, log_entry=None):
    """Extract text from PDF files."""
    try:
        with open(file_path, 'rb') as file:
            reader = PdfReader(file)
            text = "".join(page.extract_text() or "" for page in reader.pages)
        return text
    except Exception as e:
        if log_entry:
            log_entry.log(f"PDF extraction error: {str(e)}", severity=Severity.ERROR)
        return None

def extract_html_text(file_path, log_entry=None):
    """Extract text from HTML files."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file, 'html.parser')
            text = soup.get_text().strip()
            text = os.linesep.join([s for s in text.splitlines() if s.strip()])
        return text
    except Exception as e:
        if log_entry:
            log_entry.log(f"HTML extraction error: {str(e)}", severity=Severity.ERROR)
        return None

def extract_txt_text(file_path, log_entry=None):
    """Extract text from TXT files with various encodings."""
    try:
        encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        text = None

        for encoding in encodings_to_try:
            try:
                with open(file_path, 'r', encoding=encoding) as file:
                    text = file.read()
                if log_entry:
                    log_entry.log(f"Successfully read file with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue

        return text
    except Exception as e:
        if log_entry:
            log_entry.log(f"TXT extraction error: {str(e)}")
        return None

def extract_json_text(file_path, log_entry=None):
    """Extract text from JSON files."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            raw_content = file.read()
        try:
            json_data = json.loads(raw_content)
            text = json.dumps(json_data, indent=2)
        except json.JSONDecodeError as json_err:
            text = raw_content
            if log_entry:
                log_entry.log(f"JSON parsing error: {str(json_err)}. Using raw content.", severity=Severity.ERROR)
        return text
    except Exception as e:
        if log_entry:
            log_entry.log(f"JSON extraction error: {str(e)}", severity=Severity.ERROR)
        return None

def get_file_processor(file_extension):
    """Return the appropriate processor function based on file extension."""
    processors = {
        "pdf": (extract_pdf_text, "PDF"),
        "html": (extract_html_text, "HTML"),
        "txt": (extract_txt_text, "Text"),
        "json": (extract_json_text, "JSON")
    }
    return processors.get(file_extension.lower(), (None, None))