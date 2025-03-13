### multiple files

import re
import os
import glob
from Levenshtein import distance, ratio
from typing import List, Dict, Tuple, Optional, Union
import pandas as pd


class Evaluator:
    def __init__(self, gt_dir: str, pred_dir: str, file_pattern: str = "*.md"):
        """
        Initialize the evaluator with directories containing ground truth and prediction markdown files.
        
        Args:
            gt_dir (str): Directory containing ground truth markdown files
            pred_dir (str): Directory containing prediction markdown files
            file_pattern (str): File pattern to match (default: "*.md")
        """
        self.gt_dir = gt_dir
        self.pred_dir = pred_dir
        self.file_pattern = file_pattern
        
        self.gt_files = self._get_files(gt_dir, file_pattern)
        self.pred_files = self._get_files(pred_dir, file_pattern)
        
        #print(self.pred_files)
        
        self.matched_files = self._match_files()
        print(f"Found {len(self.matched_files)} matching file pairs")
        
        self.results = {}
    
    def _get_files(self, directory: str, pattern: str) -> Dict[str, str]:
        """
        Get all files matching the pattern in the directory.
        
        Args:
            directory (str): Directory to search
            pattern (str): File pattern to match
            
        Returns:
            Dict[str, str]: Dictionary mapping filename (without extension) to full path
        """
        files = {}
        for file_path in glob.glob(os.path.join(directory, pattern)):
            filename = os.path.basename(file_path)
            name_without_ext = os.path.splitext(filename)[0]
            files[name_without_ext] = file_path
        return files
    
    def _match_files(self) -> List[Tuple[str, str, str]]:
        """
        Match ground truth and prediction files by name.
        
        Returns:
            List[Tuple[str, str, str]]: List of tuples (file_id, gt_path, pred_path)
        """
        matched = []
        for file_id in self.gt_files:
            if file_id in self.pred_files:
                matched.append((file_id, self.gt_files[file_id], self.pred_files[file_id]))
        return matched
    
    def _read_file(self, file_path: str) -> str:
        """
        Read file content.
        
        Args:
            file_path (str): Path to the file
            
        Returns:
            str: File content
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def _extract_tables(self, text: str) -> List[str]:
        """
        Extract LaTeX tables from text.
        
        Args:
            text (str): Text to extract tables from
            
        Returns:
            List[str]: List of extracted tables
        """
        # For Markdown tables
        md_table_pattern = r'(\|.*\|[\r\n]+(?:\|[-:| ]+\|[\r\n]+)(?:\|.*\|[\r\n]+)+)'
        
        # For LaTeX tables
        latex_table_pattern = r"(\\begin\{table\*?\}.*?\\end\{table\*?\}|\\begin\{tabular\}.*?\\end\{tabular\})"
        
        md_tables = re.findall(md_table_pattern, text, re.MULTILINE)
        latex_tables = re.findall(latex_table_pattern, text, re.DOTALL)
        
        tables = md_tables + latex_tables
        return tables
    
    def _extract_formulas(self, text: str) -> List[str]:
        """
        Extract LaTeX formulas from text.
        
        Args:
            text (str): Text to extract formulas from
            
        Returns:
            List[str]: List of extracted formulas
        """
        # For Markdown inline math ($ $) and display math ($$ $$)
        md_formula_pattern = r'(\$\$.*?\$\$|\$[^\$].*?[^\$]\$)'
        
        # For LaTeX formulas
        latex_formula_pattern = r'(\\\[.*?\\\]|\\\(.*?\\\)|\\begin\{equation\*?\}.*?\\end\{equation\*?\}|\\begin\{align\*?\}.*?\\end\{align\*?\}|\\begin\{displaymath\}.*?\\end\{displaymath\})'
        
        md_formulas = re.findall(md_formula_pattern, text, re.DOTALL)
        latex_formulas = re.findall(latex_formula_pattern, text, re.DOTALL)
        
        formulas = md_formulas + latex_formulas
        return formulas
    
    def evaluate_file(self, file_id: str, gt_path: str, pred_path: str) -> Dict:
        """
        Evaluate a single file pair.
        
        Args:
            file_id (str): File identifier
            gt_path (str): Path to ground truth file
            pred_path (str): Path to prediction file
            
        Returns:
            Dict: Evaluation metrics for the file
        """
        gt_text = self._read_file(gt_path)
        pred_text = self._read_file(pred_path)
        

        gt_tables = self._extract_tables(gt_text)
        pred_tables = self._extract_tables(pred_text)
        
        gt_formulas = self._extract_formulas(gt_text)
        pred_formulas = self._extract_formulas(pred_text)
        

        overall_similarity = ratio(gt_text, pred_text)
        
        # Calculate table metrics
        table_similarities = []
        for gt_table in gt_tables:
            best_match = 0
            for pred_table in pred_tables:
                sim = ratio(gt_table, pred_table)
                best_match = max(best_match, sim)
            if best_match > 0:
                table_similarities.append(best_match)
        
        avg_table_similarity = sum(table_similarities) / max(1, len(table_similarities)) if table_similarities else 0
        
        # Calculate formula metrics
        formula_similarities = []
        for gt_formula in gt_formulas:
            best_match = 0
            for pred_formula in pred_formulas:
                sim = ratio(gt_formula, pred_formula)
                best_match = max(best_match, sim)
            if best_match > 0:
                formula_similarities.append(best_match)
        
        avg_formula_similarity = sum(formula_similarities) / max(1, len(formula_similarities)) if formula_similarities else 0
        
        return {
            "file_id": file_id,
            "overall_similarity": overall_similarity,
            "tables": {
                "gt_count": len(gt_tables),
                "pred_count": len(pred_tables),
                "count_difference": abs(len(gt_tables) - len(pred_tables)),
                "average_similarity": avg_table_similarity
            },
            "formulas": {
                "gt_count": len(gt_formulas),
                "pred_count": len(pred_formulas),
                "count_difference": abs(len(gt_formulas) - len(pred_formulas)),
                "average_similarity": avg_formula_similarity
            }
        }
    
    def evaluate_all(self) -> Dict:
        """
        Evaluate all matched file pairs.
        
        Returns:
            Dict: Evaluation results for all files
        """
        file_results = []
        
        for file_id, gt_path, pred_path in self.matched_files:
            print(f"Evaluating {file_id}...")
            result = self.evaluate_file(file_id, gt_path, pred_path)
            file_results.append(result)
        

        overall_similarities = [r["overall_similarity"] for r in file_results]
        table_similarities = [r["tables"]["average_similarity"] for r in file_results]
        formula_similarities = [r["formulas"]["average_similarity"] for r in file_results]
        
        self.results = {
            "file_count": len(file_results),
            "file_results": file_results,
            "aggregate": {
                "average_overall_similarity": sum(overall_similarities) / max(1, len(overall_similarities)),
                "average_table_similarity": sum(table_similarities) / max(1, len(table_similarities)),
                "average_formula_similarity": sum(formula_similarities) / max(1, len(formula_similarities))
            }
        }
        
        return self.results
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert results to pandas DataFrame for easier analysis.
        
        Returns:
            pd.DataFrame: DataFrame with evaluation results
        """
        if not self.results or not self.results.get("file_results"):
            self.evaluate_all()
            
        data = []
        for result in self.results["file_results"]:
            row = {
                "file_id": result["file_id"],
                "overall_similarity": result["overall_similarity"],
                "gt_tables": result["tables"]["gt_count"],
                "pred_tables": result["tables"]["pred_count"],
                "table_diff": result["tables"]["count_difference"],
                "table_similarity": result["tables"]["average_similarity"],
                "gt_formulas": result["formulas"]["gt_count"],
                "pred_formulas": result["formulas"]["pred_count"],
                "formula_diff": result["formulas"]["count_difference"],
                "formula_similarity": result["formulas"]["average_similarity"]
            }
            data.append(row)
            
        return pd.DataFrame(data)
    
    def save_results(self, output_path: str) -> None:
        """
        Save results to CSV file.
        
        Args:
            output_path (str): Path to save results
        """
        df = self.to_dataframe()
        df.to_csv(output_path, index=False)
        print(f"Results saved to {output_path}")

evaluator = Evaluator("../data/truths/", "../data/predictions/mathpix/", "*.md")
results = evaluator.evaluate_all()
evaluator.save_results("evaluation_results_mathpix.csv")