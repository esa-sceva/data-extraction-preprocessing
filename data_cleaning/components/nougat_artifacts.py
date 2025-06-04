from typing import Optional

import re

from model.base import DataProcessingComponent
from helper.logger import Logger

class NougatArtifactRemovalComponent(DataProcessingComponent):

    def __init__(self, debug: bool = False):
        super().__init__(debug = debug)

    def process(self, content: str, logger: Logger, filename: str) -> Optional[str]:
        if self.debug:
            logger.log(f"[DEBUG] Before NougatArtifactRemovalComponent ({filename}):\n{content[:500]}{'...' if len(content) > 500 else ''}")

        if not content:
            logger.log(f"[ERROR] {filename} - Empty content in Nougat artifact removal")
            return None
        try:
            cleaned = re.sub(r'\+\+\+\s*==WARNING: Truncated because of repetitions==.*?\+\+\+',
                            '', content, flags=re.DOTALL)
            cleaned = re.sub(r'\+\+\+\s*==ERROR: No output for this page==.*?\+\+\+',
                            '', cleaned, flags=re.DOTALL)
            cleaned = cleaned.replace('[MISSING_PAGE_POST]', '')
            logger.log(f"[SUCCESS] {filename} - Removed Nougat artifacts")
            if self.debug:
                logger.log(f"[DEBUG] After NougatArtifactRemovalComponent ({filename}):\n{cleaned[:500]}{'...' if len(cleaned) > 500 else ''}")
            return cleaned
        except Exception as e:
            logger.log(f"[ERROR] {filename} - Nougat artifact removal failed: {str(e)}")
            if self.debug: 
                logger.log(f"[DEBUG] NougatArtifactRemovalComponent ({filename}) - Returning original content due to error:\n{content[:500]}{'...' if len(content) > 500 else ''}")
            return content