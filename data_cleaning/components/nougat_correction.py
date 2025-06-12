from typing import Optional
import re
from colorama import Fore, Style, init

from model.base import DataProcessingComponent
from helper.logger import Logger

from .nougat_helpers import postprocess_single
# Initialize colorama
init()


class NougatCorrection(DataProcessingComponent):

    def __init__(self, debug: bool = False):
        super().__init__(debug = debug)
    
    @staticmethod
    def _clean_latex_table(raw_table: str) -> str:
        # Replace escaped backslashes like \\hline with \hline
        table = re.sub(r'\\\\(hline)', r'\\\1', raw_table)

        # Fix row endings: Replace `\\\\` with `\\`
        table = re.sub(r'\\\\+', r'\\\\', table)

        # Format math expressions like \(a_{min}\) properly
        table = re.sub(r'\\\((.*?)\\\)', r'\\(\1\\)', table)

        # use repr to see the actual content 
        # table = table.replace('\\\\begin{table}', '\begin{table}')

        table = re.sub(r'\\\\multicolumn', r'\\multicolumn', table)
        
        table = table.replace('\\\\begin{tabular}', '\\begin{tabular}')

        table = table.replace('\\\\end{table}', '\end{table}')
        table = table.replace('\\\\end{tabular}', '\end{tabular}')

        return table
    
    def process(self, content: str, logger: Logger, filename: str) -> Optional[str]:
        if self.debug:
            print(f"{Fore.YELLOW}[DEBUG] Before Nougat Correction ({filename}):{Style.RESET_ALL}\n{content[:500]}{'...' if len(content) > 500 else ''}")

        if not content:
            logger.log(f"[ERROR] {filename} - Empty content in Nougat Correction ")
            return None
        try:
            # first apply nougats actual postprocessing
            cleaned = postprocess_single(content, markdown_fix = True)

            #now apply custom table logic
            cleaned = NougatCorrection._clean_latex_table(cleaned)
            
            logger.log(f"[SUCCESS] {filename} - Fixed Nougat Correction ")
            if self.debug:
                print(f"\n{Fore.GREEN}[DEBUG] After Nougat Correction  ({filename}):{Style.RESET_ALL}\n{cleaned[:500]}{'...' if len(cleaned) > 500 else ''}")
            return cleaned
        except Exception as e:
            logger.log(f"[ERROR] {filename} - Nougat Correction failed: {str(e)}")
            return content