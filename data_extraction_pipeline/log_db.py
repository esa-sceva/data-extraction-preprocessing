import dataclasses
import os
import time
from pathlib import Path

from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import mysql.connector



# Load environment variables from the .env file
load_dotenv()
Base = declarative_base()

# Define enum for severity
from enum import Enum
class Severity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"

import mysql.connector
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

@dataclasses.dataclass
class LogEntry:
    log_id: int = None
    log_text: str = None
    filename: str = None
    timestamp: str = None
    provider: str = None
    file_path: str = None
    status: str = 'in progress'
    log_format: str = '{timestamp} - {severity}:\n{log}'
    extracted_text_length: int = 0

    def save(self):
        """Save the log entry to the database. If log_id is None, insert a new entry, otherwise update."""
        try:
            # Get sensitive information from environment variables
            db_host = os.getenv("DB_HOST")
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_name = os.getenv("DB_NAME")
            db_table = os.getenv("DB_TABLE")

            # Establish MySQL connection
            connection = mysql.connector.connect(
                host=db_host,
                user=db_user,
                password=db_password,
                database=db_name
            )

            cursor = connection.cursor()

            if self.log_id is None:
                # If no log_id, insert a new entry
                cursor.execute(f"""
                    INSERT INTO {db_table} (filename, log, provider, status, file_path, extracted_text_length, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, (self.filename, self.log_text, self.provider, self.status, self.file_path, 0))
                self.log_id = cursor.lastrowid
            else:
                # If log_id exists, update the existing entry
                # Option 1: Use string formatting before passing to execute
                query = f"""
                    UPDATE {db_table}
                    SET log = %s, status = %s, extracted_text_length = %s
                    WHERE id = %s
                """
                cursor.execute(query, (self.log_text, self.status, self.extracted_text_length, self.log_id))

            # Commit changes to the database
            connection.commit()

            # Close the connection
            cursor.close()
            connection.close()

        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def log(self, new_message, severity=Severity.INFO):
        """Append a new message to the existing log entry."""
        if self.log_id is None:
            raise ValueError("Cannot append to a non-existent log entry.")

        new_message = self.log_format.format(log=new_message, severity=severity.value, timestamp=time.strftime('%Y-%m-%d %H:%M:%S'))
        self.log_text += f"\n{new_message}"
        self.save()

    def finalize_log(self, status="success", extracted_text_length=0):
        """Finalize the log entry by updating its status."""
        if self.log_id is None:
            raise ValueError("Cannot finalize a non-existent log entry.")

        self.status = status
        self.extracted_text_length = extracted_text_length
        self.save()


    @classmethod
    def start_new(cls, filename: str, file_path: Path, log_text: str, provider: str):
        """Start a new log entry and return the LogEntry instance."""
        log_entry = cls(filename=filename, log_text=log_text, provider=provider, file_path=file_path.as_posix())
        log_entry.save()
        return log_entry
