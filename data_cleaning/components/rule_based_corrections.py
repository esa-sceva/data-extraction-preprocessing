from typing import Optional
import re
from colorama import Fore, Style, init

from model.base import DataProcessingComponent
from helper.logger import Logger

# Initialize colorama
init()


class RuleBasedCorrections(DataProcessingComponent):

    def __init__(self, debug: bool = False):
        super().__init__(debug = debug)
    
    def process(self, content: str, logger: Logger, filename: str) -> Optional[str]:
        if self.debug:
            print(f"{Fore.YELLOW}[DEBUG] Before Rule Based Correction ({filename}):{Style.RESET_ALL}\n{content[:500]}{'...' if len(content) > 500 else ''}")

        if not content:
            logger.log(f"[ERROR] {filename} - Empty content in Rule Based Correction ")
            return None
        try:
            cleaned_lines = []
            for line in content.split('\n'):
                stripped = line.strip()

                # Skip if line has no alphanumeric characters (i.e., only symbols/punctuation) and single
                if not re.search(r'\w', stripped) and len(stripped) == 1:
                    continue
                
                cleaned_lines.append(line)

            cleaned = '\n'.join(cleaned_lines)
            # replace 3+ consecutive newlines with exactly 2 if present
            cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)

            # remove any leading or trailing whitespaces
            cleaned = cleaned.strip()
            logger.log(f"[SUCCESS] {filename} - Fixed Rule Based Correction ")
            if self.debug:
                print(f"\n{Fore.GREEN}[DEBUG] After Rule Based Correction  ({filename}):{Style.RESET_ALL}\n{cleaned[:500]}{'...' if len(cleaned) > 500 else ''}")
            return cleaned
        except Exception as e:
            logger.log(f"[ERROR] {filename} - Rule Based Correction failed: {str(e)}")
            return content