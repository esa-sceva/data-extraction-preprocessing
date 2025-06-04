"""a simple logger file stored under logs/"""

from datetime import datetime
from pathlib import Path

class Logger:
    def __init__(self, filename: str):
        self.log_path = Path("logs")
        self.log_path.mkdir(exist_ok=True)
        self.file = self.log_path / f"{filename}.log"

    def log(self, message: str):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.file, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {message}\n")