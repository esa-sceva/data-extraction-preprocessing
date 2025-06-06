from typing import Optional
import re
from colorama import Fore, Style, init

from model.base import DataProcessingComponent
from helper.logger import Logger

# Initialize colorama
init()


class OCRCorrections(DataProcessingComponent):

    def __init__(self, debug: bool = False):
        super().__init__(debug = debug)
    
    def process(self, content: str, logger: Logger, filename: str) -> Optional[str]:
        if self.debug:
            print(f"{Fore.YELLOW}[DEBUG] Before OCRCorrections ({filename}):{Style.RESET_ALL}\n{content[:500]}{'...' if len(content) > 500 else ''}")

        if not content:
            logger.log(f"[ERROR] {filename} - Empty content in OCRCorrections")
            return None
        try:
            cleaned = re.sub(r'(\d+)([A-Za-z])', r'\1 \2', content) # add space between number and text
            # cleaned = re.sub(r'(\d+)([A-Za-z])', r'\1 \2', cleaned) # if needed add a full stop
            logger.log(f"[SUCCESS] {filename} - Fixed OCRCorrections")
            if self.debug:
                print(f"\n{Fore.GREEN}[DEBUG] After OCRCorrections ({filename}):{Style.RESET_ALL}\n{cleaned[:500]}{'...' if len(cleaned) > 500 else ''}")
            return cleaned
        except Exception as e:
            logger.log(f"[ERROR] {filename} - OCRCorrections failed: {str(e)}")
            return content