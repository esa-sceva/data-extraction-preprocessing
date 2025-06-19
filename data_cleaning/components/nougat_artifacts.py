from typing import Optional
import re
from colorama import Fore, Style, init

from model.base import DataProcessingComponent
from helper.logger import Logger

# Initialize colorama
init()

class NougatArtifactRemovalComponent(DataProcessingComponent):

    def __init__(self, debug: bool = False):
        super().__init__(debug = debug)

    def process(self, content: str, logger: Logger, filename: str) -> Optional[str]:
        if self.debug:
            print(f"{Fore.YELLOW}[DEBUG] Before NougatArtifactRemovalComponent ({filename}):{Style.RESET_ALL}\n{content[:500]}{'...' if len(content) > 500 else ''}")

        if not content:
            logger.log(f"[ERROR] {filename} - Empty content in Nougat artifact removal")
            return None
        try:
            # two additional checks to add
            cleaned = content.strip('"')
            # Replace the escaped "\n" with actual newline characters
            cleaned = cleaned.replace('\\n', '\n')

            cleaned = re.sub(r'\+\+\+\s*==WARNING: Truncated because of repetitions==.*?\+\+\+',
                            '', cleaned, flags=re.DOTALL)
            cleaned = re.sub(r'\+\+\+\s*==ERROR: No output for this page==.*?\+\+\+',
                            '', cleaned, flags=re.DOTALL)
            cleaned = cleaned.replace('[MISSING_PAGE_POST]', '')
            logger.log(f"[SUCCESS] {filename} - Removed Nougat artifacts")
            if self.debug:
                print(f"\n{Fore.GREEN}[DEBUG] After NougatArtifactRemovalComponent ({filename}):{Style.RESET_ALL}\n{cleaned[:500]}{'...' if len(cleaned) > 500 else ''}")
            return cleaned
        except Exception as e:
            logger.log(f"[ERROR] {filename} - Nougat artifact removal failed: {str(e)}")
            return content