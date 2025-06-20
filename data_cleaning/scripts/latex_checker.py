# This script uses pylatex to compile latex equations and tables and logs issues if found.

import logging
import multiprocessing
import os
import re
import subprocess
import tempfile
import time
from tqdm.auto import tqdm
from multiprocessing import Pool
from pathlib import Path
from typing import Tuple

logging.basicConfig(
    filename ='formula_checker.log',
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s'
)

class LatexFormulaChecker:
    def __init__(self):
        # Standard inline formula pattern
        self.inline_pattern = re.compile(r'(?<!\$)\$(?!\$)(.+?)(?<!\$)\$(?!\$)') # match $ enclosed by $ if not followed by another $

        # Standard display formula patterns
        self.display_pattern = re.compile(r'\$\$(.*?)\$\$', re.DOTALL) # match doubel $$
        self.bracket_pattern = re.compile(r'\\[(](.*?)\\[)]', re.DOTALL)  # \( ... \)
        self.square_bracket_pattern = re.compile(r'\\\[(.*?)\\\]', re.DOTALL)  # \[ ... \]

        # LaTeX environment patterns(mostly math env)
        self.latex_env_pattern = re.compile(r'\\begin\{([^}]+)\}(.*?)\\end\{\1\}', re.DOTALL)

        # LaTeX table environments 
        self.table_env_pattern = re.compile(
            r'\\begin\{(table)\}(.*?)\\end\{table\}',  # Captures entire table
            re.DOTALL
        )

    def extract_formulas(self, markdown_text):
        """Extract LaTeX formulas from markdown text."""
        formulas = []

        # Extract inline formulas ($...$)
        for match in self.inline_pattern.finditer(markdown_text):
            formulas.append(('inline', match.group(1)))

        # Extract display formulas ($$...$$)
        for match in self.display_pattern.finditer(markdown_text):
            formulas.append(('display', match.group(1)))

        # Extract \( ... \) formulas
        for match in self.bracket_pattern.finditer(markdown_text):
            formulas.append(('inline-explicit', match.group(1)))

        # Extract \[ ... \] formulas
        for match in self.square_bracket_pattern.finditer(markdown_text):
            formulas.append(('display-explicit', match.group(1)))

        # Extract formulas with \begin{...}...\end{...}
        for match in self.latex_env_pattern.finditer(markdown_text):
            env_type = match.group(1)
            if env_type in ['equation', 'align', 'gather', 'multline', 'eqnarray', 'matrix', 'equation*', 'align*']:
                formulas.append((f'env:{env_type}', match.group(2)))
        
        # Extract full table environments (including nested tabular)
        for match in self.table_env_pattern.finditer(markdown_text):
            formulas.append(('table-env:', match.group(0)))  # Entire table block
                
        return formulas

    def check_formula_syntax(self, formula, formula_type='inline'):
        """Check if a LaTeX formula has valid syntax using a minimal LaTeX document."""
        if formula.strip() == '':
            return True, "Empty formula"

        # Create a minimal LaTeX document to test the formula
        if formula_type == 'inline':
            test_content = r"\documentclass{article}\usepackage{amsmath}\usepackage{amssymb}\usepackage{multirow}\usepackage{bm}\begin{document}$" + formula + r"$\end{document}"
        elif formula_type == 'inline-explicit':
            test_content = r"\documentclass{article}\usepackage{amsmath}\usepackage{amssymb}\usepackage{multirow}\usepackage{bm}\begin{document}\(" + formula + r"\)\end{document}"
        elif formula_type == 'display':
            test_content = r"\documentclass{article}\usepackage{amsmath}\usepackage{amssymb}\usepackage{multirow}\usepackage{bm}\begin{document}$$" + formula + r"$$\end{document}"
        elif formula_type == 'display-explicit':
            test_content = r"\documentclass{article}\usepackage{amsmath}\usepackage{amssymb}\usepackage{multirow}\usepackage{bm}\begin{document}\[" + formula + r"\]\end{document}"
        elif formula_type.startswith('env:'):
            env = formula_type.split(':')[1]
            test_content = r"\documentclass{article}\usepackage{amsmath}\usepackage{amssymb}\usepackage{multirow}\usepackage{bm}\begin{document}\begin{" + env + "}" + formula + r"\end{" + env + "}\end{document}"
        elif formula_type.startswith('table-env:'):
            test_content = r"\documentclass{article}\usepackage{amsmath}\usepackage{amssymb}\usepackage{multirow}\usepackage{bm}\begin{document}" + formula + r"\end{document}"

        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as tmp_dir:
            tex_file = os.path.join(tmp_dir, "test.tex")
            with open(tex_file, 'w') as f:
                f.write(test_content)

            # Run pdflatex to check for errors
            process = subprocess.run(
                ['pdflatex', '-interaction=nonstopmode', '-halt-on-error', tex_file],
                cwd=tmp_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Check if compilation was successful
            if process.returncode == 0:
                return True, "Formula syntax is valid"
            else:
                # Extract error message
                error_lines = process.stdout.split('\n')
                error_msg = "Unknown error"
                for i, line in enumerate(error_lines):
                    if "! " in line:  # LaTeX error lines start with !
                        error_msg = line.strip()
                        if i + 1 < len(error_lines) and error_lines[i + 1].strip():
                            error_msg += " " + error_lines[i + 1].strip()
                        break

                return False, error_msg

def process_file(file_path: Path) -> Tuple[str, int, int, int]:
    """
    Process a single Markdown file and check its LaTeX formulas.
    Returns: (filename, total_formulas, correct_formulas, incorrect_formulas)
    """
    checker = LatexFormulaChecker()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = f.read()
    except Exception as e:
        logging.error(f"Error with {file_path}: {str(e)}")
        return (str(file_path), 0, 0, 0)

    formulas = checker.extract_formulas(data)
    total_formulas = len(formulas)
    correct_formulas = 0
    incorrect_formulas = 0

    for formula_type, formula in formulas:
        valid, message = checker.check_formula_syntax(formula, formula_type)
        if valid:
            correct_formulas += 1
        else:
            incorrect_formulas += 1
            logging.warning(f"Invalid formula in {file_path} (type: {formula_type}): {formula} - Error: {message}")

    logging.info(f"File: {file_path}, Total formulas: {total_formulas}, Correct: {correct_formulas}, Incorrect: {incorrect_formulas}")
    return (str(file_path), total_formulas, correct_formulas, incorrect_formulas)

def main(directory: str, num_processes: int = 4):
    """
    Main function to process all Markdown files in a directory using multiprocessing.
    """
    start_time = time.time() 
    directory_path = Path(directory)

    md_files = list(directory_path.glob('*.md'))
    if not md_files:
        logging.info(f"No Markdown files found in {directory}")
        print(f"No Markdown files found in {directory}")
        return

    print(f"Found {len(md_files)} Markdown files to process")
    print(f"Using {num_processes} workers")

    # with Pool(processes = num_processes) as pool:
    #     results = pool.map(process_file, md_files)
    
    with Pool(processes = num_processes) as pool:
        results = list(tqdm(pool.imap(process_file, md_files), total=len(md_files), desc="Processing files"))

    total_files = len(results)
    total_formulas_all = sum(r[1] for r in results)
    total_correct = sum(r[2] for r in results)
    total_incorrect = sum(r[3] for r in results)

    end_time = time.time()
    elapsed_time = end_time - start_time

    print("\nOverall Summary:")
    print(f"Total files processed: {total_files}")
    print(f"Total formulas: {total_formulas_all}")
    print(f"Correct formulas: {total_correct}")
    print(f"Incorrect formulas: {total_incorrect}")
    print(f"Total time taken: {elapsed_time:.2f} seconds")

    logging.info(
        f"Overall Summary: Files: {total_files}, Total formulas: {total_formulas_all}, "
        f"Correct: {total_correct}, Incorrect: {total_incorrect}, Time taken: {elapsed_time:.2f} seconds"
    )

if __name__ == "__main__":
    directory_to_process = "/content/test"
    cpu_count = multiprocessing.cpu_count()
    main(directory_to_process, num_processes = cpu_count)