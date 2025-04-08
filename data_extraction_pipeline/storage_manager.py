import os
import boto3
import datetime
import re
from log_db import Severity

def get_safe_filename(key):
    """Convert a filename to a safe format."""
    base_name = os.path.basename(key)
    base_name = os.path.splitext(base_name)[0]
    safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
    return safe_name

def save_extracted_markdown(key, extracted_text, file_type, subdir_name, save_to_local, bucket_name, destination_bucket, log_entry=None):
    """Save extracted text as markdown to local or S3."""
    try:
        base_filename = get_safe_filename(key)
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_path = f"{destination_bucket}/{subdir_name}/{base_filename}.md"

        if save_to_local:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(extracted_text)
        else:
            client = boto3.client(
                "s3",
                region_name=os.getenv("AWS_REGION"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
            )
            client.put_object(
                Bucket=bucket_name,
                Key=file_path,
                Body=extracted_text.encode('utf-8'),
                ContentType='text/markdown'
            )
        if log_entry:
            log_entry.log(f"Saved markdown to {'local' if save_to_local else 'S3'}: {file_path}")
        return True
    except Exception as e:
        if log_entry:
            log_entry.log(f"Error saving markdown: {str(e)}", severity=Severity.ERROR)
        return False