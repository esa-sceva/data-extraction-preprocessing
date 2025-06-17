# This script uses pylatex to compile latex equations and tables and logs issues if found.

import re
import os
import subprocess
import tempfile
import logging

logging.basicConfig(
    filename ='formula_checker.log',
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s'
)

class LatexFormulaChecker:
    def __init__(self):
        # Standard inline formula pattern
        self.inline_pattern = re.compile(r'(?<!\$)\$(?!\$)(.+?)(?<!\$)\$(?!\$)')

        # Standard display formula patterns
        self.display_pattern = re.compile(r'\$\$(.*?)\$\$', re.DOTALL)
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
            test_content = r"\documentclass{article}\usepackage{amsmath}\begin{document}$" + formula + r"$\end{document}"
        elif formula_type == 'inline-explicit':
            test_content = r"\documentclass{article}\usepackage{amsmath}\begin{document}\(" + formula + r"\)\end{document}"
        elif formula_type == 'display':
            test_content = r"\documentclass{article}\usepackage{amsmath}\begin{document}$$" + formula + r"$$\end{document}"
        elif formula_type == 'display-explicit':
            test_content = r"\documentclass{article}\usepackage{amsmath}\begin{document}\[" + formula + r"\]\end{document}"
        elif formula_type.startswith('env:'):
            env = formula_type.split(':')[1]
            test_content = r"\documentclass{article}\usepackage{amsmath}\begin{document}\begin{" + env + "}" + formula + r"\end{" + env + "}\end{document}"
        elif formula_type.startswith('table-env:'):
            test_content = r"\documentclass{article}\usepackage{amsmath}\begin{document}" + formula + r"\end{document}" # dont add $

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

def main():
    checker = LatexFormulaChecker()
    filename = "/content/018a3eaf_368f_4053_9f77_abfb592b5251.md"
    
    try:
        with open(filename, 'r') as f:
            data = f.read()
    except FileNotFoundError:
        logging.error(f"File not found: {filename}")
        print(f"Error: File {filename} not found")
        return

    formulas = checker.extract_formulas(data)

    # print(formulas)
    total_formulas = len(formulas)

    correct_formulas = 0
    incorrect_formulas = 0

    for i, (formula_type, formula) in enumerate(formulas):
        valid, message = checker.check_formula_syntax(formula, formula_type)
        if valid:
            correct_formulas += 1
        else:
            incorrect_formulas += 1
            logging.warning(f"Invalid formula in {filename} (type: {formula_type}): {formula} - Error: {message}")

    # Log summary
    logging.info(f"File: {filename}, Total formulas: {total_formulas}, Correct: {correct_formulas}, Incorrect: {incorrect_formulas}")
    print(f"Summary - Total formulas: {total_formulas}, Correct: {correct_formulas}, Incorrect: {incorrect_formulas}")

if __name__ == "__main__":
    main()