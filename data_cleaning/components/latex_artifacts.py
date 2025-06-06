from typing import List, Optional
import re
from colorama import Fore, Style, init

from model.base import DataProcessingComponent
from helper.logger import Logger

# Initialize colorama
init()

class LatexExtractor(DataProcessingComponent):

    def __init__(self, debug: bool = False):
        super().__init__(debug = debug)

    def _find_latex_environments(self, text: str) -> List[tuple[int, int]]:
        environments = []
        pos = 0
        while True:
            begin_pos = text.find("\\begin{", pos)
            if begin_pos == -1:
                break
            end_pos = self._find_matching_end(text, begin_pos)
            if end_pos == -1:
                pos = begin_pos + 6
                continue
            environments.append((begin_pos, end_pos))
            pos = end_pos
        return environments

    def _find_matching_end(self, text: str, begin_pos: int) -> int:
        begin_match = re.search(r'\\begin\{([^}]+)\}', text[begin_pos:])
        if not begin_match:
            return -1
        env_name = begin_match.group(1)
        env_begin = f"\\begin{{{env_name}}}"
        env_end = f"\\end{{{env_name}}}"
        current_pos = begin_pos + len(env_begin)
        nesting_level = 1
        while nesting_level > 0 and current_pos < len(text):
            begin_idx = text.find(env_begin, current_pos)
            end_idx = text.find(env_end, current_pos)
            if end_idx == -1:
                return -1
            if begin_idx == -1 or end_idx < begin_idx:
                nesting_level -= 1
                current_pos = end_idx + len(env_end)
            else:
                nesting_level += 1
                current_pos = begin_idx + len(env_begin)
        return current_pos if nesting_level == 0 else -1

    def _extract_latex(self, text: str) -> List[str]:
        latex_contents = []
        env_spans = self._find_latex_environments(text)
        for start, end in env_spans:
            latex_contents.append(text[start:end])
        latex_formula_pattern = re.compile(r'\${1,2}\s*[^$]+\s*\${1,2}|\\\[[\s\S]*?\\\]|\\\([\s\S]*?\\\)')
        for match in latex_formula_pattern.finditer(text):
            is_inside_env = any(start <= match.start() and match.end() <= end
                              for start, end in env_spans)
            if not is_inside_env:
                latex_contents.append(match.group(0))
        return latex_contents
    
    def process(self, content: str, logger: Logger, filename: str) -> Optional[str]:
        if self.debug:
            print(f"{Fore.YELLOW}[DEBUG] Before LatexExtractor ({filename}):{Style.RESET_ALL}\n{content[:500]}{'...' if len(content) > 500 else ''}")

        if not content:
            logger.log(f"[ERROR] {filename} - Empty content in LatexExtractor removal")
            return None
        try:
            latex_elements = self._extract_latex(content)
            for i, latex in enumerate(latex_elements, 1):
                print(f"LaTeX element {i}: {latex.strip()}")
                content = content.replace(latex, "")
            logger.log(f"[SUCCESS] {filename} - Removed LatexExtractor artifacts")
            if self.debug:
                print(f"\n{Fore.GREEN}[DEBUG] After LatexExtractor ({filename}):{Style.RESET_ALL}\n{content[:500]}{'...' if len(content) > 500 else ''}")
            return content
        except Exception as e:
            logger.log(f"[ERROR] {filename} - LatexExtractor removal failed: {str(e)}")
            return content