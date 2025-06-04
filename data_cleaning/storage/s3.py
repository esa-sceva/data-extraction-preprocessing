import os
import re
import boto3

from helper.logger import Logger
from model.base import DataStorageComponent

class LocalStorageComponent(DataStorageComponent):
    def __init__(self, destination_bucket: str):
        self.destination_bucket = destination_bucket

    def save(self, key: str, content: str, subdir_name: str, logger: Logger) -> None:
        try:
            base_filename = self.get_safe_filename(key)
            file_path = f"{self.destination_bucket}/{subdir_name}/{base_filename}.md"
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.log(f"[SAVE] File saved to local: {file_path}")
        except Exception as e:
            logger.log(f"[ERROR] Failed to save to local: {str(e)}")

    @staticmethod
    def get_safe_filename(key: str) -> str:
        base_name = os.path.basename(key)
        base_name = os.path.splitext(base_name)[0]
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        return safe_name

class S3StorageComponent(DataStorageComponent):
    def __init__(self, bucket_name: str, destination_bucket: str):
        self.bucket_name = bucket_name
        self.destination_bucket = destination_bucket
        self.client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )

    def save(self, key: str, content: str, subdir_name: str, logger: Logger) -> None:
        try:
            base_filename = self.get_safe_filename(key)
            file_path = f"{self.destination_bucket}/{subdir_name}/{base_filename}.md"
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=file_path,
                Body=content.encode('utf-8'),
                ContentType='text/markdown'
            )
            logger.log(f"[SAVE] File saved to S3: {file_path}")
        except Exception as e:
            logger.log(f"[ERROR] Failed to save to S3: {str(e)}")

    @staticmethod
    def get_safe_filename(key: str) -> str:
        base_name = os.path.basename(key)
        base_name = os.path.splitext(base_name)[0]
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        return safe_name