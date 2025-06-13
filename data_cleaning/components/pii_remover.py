from typing import Optional
from colorama import Fore, Style, init

import re

from .presidio_helpers import analyzer_engine, analyze

from model.base import DataProcessingComponent
from helper.logger import Logger


# Initialize colorama
init()

analyzer_params = ("flair", "flair/ner-english-large", "", "")

# def anonymize_text(text, results):
#     results_sorted = sorted(results, key = lambda r: r.start, reverse=True)
#     for res in results_sorted:
#         if res.entity_type not in ['PERSON', 'ORGANIZATION', 'EMAIL_ADDRESS']:
#             continue
#         placeholder = f"[{res.entity_type.upper()}]"
#         text = text[:res.start] + placeholder + text[res.end:]
#     return text

def anonymize_text(text, results):
    """with custom email regex handling"""

    results_sorted = sorted(results, key=lambda r: r.start, reverse=True)
    
    # Create a list of spans already covered (so we don't overlap)
    covered_spans = set()
    
    for res in results_sorted:
        if res.entity_type not in ['PERSON', 'EMAIL_ADDRESS']:
            continue
        placeholder = f"[{res.entity_type.upper()}]"
        text = text[:res.start] + placeholder + text[res.end:]
        covered_spans.update(range(res.start, res.end))

    # Regex to catch any missed email addresses
    email_pattern = re.compile(r'([-a-zA-Z0-9.`?{}]+@\w+(?:\.\w+)+)') #divided into before and after the @ part
    
    # Go through regex matches *after* handling NER results
    for match in reversed(list(email_pattern.finditer(text))):
        start, end = match.span()
        if any(i in covered_spans for i in range(start, end)):
            continue  # Skip already covered spans
        text = text[:start] + "[EMAIL_ADDRESS]" + text[end:]

    return text

class PIIRemover(DataProcessingComponent):

    def __init__(self, debug: bool = False):
        super().__init__(debug = debug)
        self.analyzer = analyzer_engine(*analyzer_params)
    
    def process(self, content: str, logger: Logger, filename: str) -> Optional[str]:
        if self.debug:
            print(f"{Fore.YELLOW}[DEBUG] Before PII Removal ({filename}):{Style.RESET_ALL}\n{content[:500]}{'...' if len(content) > 500 else ''}")

        if not content:
            logger.log(f"[ERROR] {filename} - Empty content in PII Removal")
            return None
        try:
            analyze_results = analyze(
                *analyzer_params,
                text = content,
                entities = None,  # Use all supported entities
                language = "en",
                score_threshold = 0.35,
                return_decision_process = False,
                allow_list = [],
                deny_list = []
            )

            anonymized_text = anonymize_text(content, analyze_results)

            logger.log(f"[SUCCESS] {filename} - PII Removal Done")
            if self.debug:
                print(f"\n{Fore.GREEN}[DEBUG] After PII Removal ({filename}):{Style.RESET_ALL}\n{anonymized_text[:500]}{'...' if len(anonymized_text) > 500 else ''}")
            return anonymized_text
        except Exception as e:
            logger.log(f"[ERROR] {filename} - PII Removal failed: {str(e)}")
            return content