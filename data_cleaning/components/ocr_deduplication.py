from typing import Optional, List, Tuple
import re

import nltk
from nltk import ngrams, sent_tokenize
from datasketch import MinHash, MinHashLSH
from colorama import Fore, Style, init

from model.base import DataProcessingComponent
from .latex_artifacts import LatexExtractor
from helper.logger import Logger

# Initialize colorama
init()

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')


class OCRDuplicateRemover(DataProcessingComponent):
    """
    Component to detect and remove OCR-induced duplicate text segments.
    Uses LSH (Locality Sensitive Hashing) to efficiently find near-duplicate
    paragraphs or sentences and removes redundant copies.
    """
    
    def __init__(self, 
                 shingle_size: int = 10,
                 num_perm: int = 128,
                 threshold: float = 0.6,
                 batch_size: int = 1000,
                 unit: str = 'paragraph',
                 min_words: int = 10,
                 debug: bool = False):
        """
        Initialize the OCR duplicate remover.
        
        Args:
            shingle_size: Size of n-grams for text comparison
            num_perm: Number of permutations for MinHash
            threshold: Jaccard similarity threshold for duplicates
            batch_size: Process units in batches to manage memory
            unit: 'paragraph' or 'sentence' for deduplication unit
            min_words: Minimum words required for a unit to be processed
            debug: Enable debug output
        """
        super().__init__(debug=debug)
        self.shingle_size = shingle_size
        self.num_perm = num_perm
        self.threshold = threshold
        self.batch_size = batch_size
        self.unit = unit
        self.min_words = min_words

    def process(self, content: str, logger: Logger, filename: str) -> Optional[str]:
        """
        Process content to remove OCR-induced duplicate text segments.
        
        Args:
            content: The text content to process
            logger: Logger instance for logging
            filename: Name of the file being processed
            
        Returns:
            Cleaned content with duplicates removed, or None if processing fails
        """
        if self.debug:
            print(f"{Fore.YELLOW}[DEBUG] Before OCRDuplicateRemover ({filename}):{Style.RESET_ALL}\n{content[:500]}{'...' if len(content) > 500 else ''}")

        if not content:
            logger.log(f"[ERROR] {filename} - Empty content in OCRDuplicateRemover")
            return None

        try:

            # first remove any stuff latex
            latex_component = LatexExtractor(debug = True)
            content = latex_component.process(content, filename = filename)
            
            # Extract text units (paragraphs or sentences)
            units = self._extract_units(content)
            
            if not units:
                logger.log(f"[INFO] {filename} - No units found for deduplication")
                return content

            # Find duplicate groups using LSH
            duplicate_groups = self._find_duplicates(units)
            
            if not duplicate_groups:
                logger.log(f"[INFO] {filename} - No duplicates found")
                return content

            # Identify OCR-induced duplicates (consecutive with trivial gaps)
            ocr_duplicates = self._identify_ocr_duplicates(duplicate_groups, content)
            
            if not ocr_duplicates:
                logger.log(f"[INFO] {filename} - No OCR duplicates identified")
                return content

            # Remove duplicates from content
            cleaned_content = self._remove_duplicates(content, ocr_duplicates)
            
            logger.log(f"[SUCCESS] {filename} - Removed {len(ocr_duplicates)} OCR duplicate segments")
            
            if self.debug:
                print(f"\n{Fore.GREEN}[DEBUG] After OCRDuplicateRemover ({filename}):{Style.RESET_ALL}\n{cleaned_content[:500]}{'...' if len(cleaned_content) > 500 else ''}")
            
            return cleaned_content
            
        except Exception as e:
            logger.log(f"[ERROR] {filename} - OCRDuplicateRemover failed: {str(e)}")
            return content

    def _extract_units(self, content: str) -> List[Tuple[str, str]]:
        """Extract paragraphs or sentences with position information."""
        units = []
        
        if self.unit == 'paragraph':
            segments = content.split('\n\n')
        elif self.unit == 'sentence':
            segments = sent_tokenize(content)
        else:
            raise ValueError("Unit must be 'paragraph' or 'sentence'")

        current_pos = 0
        for segment in segments:
            segment = segment.strip()
            if len(segment.split()) >= self.min_words:
                start = content.find(segment, current_pos)
                if start != -1:  # Found the segment
                    end = start + len(segment)
                    unit_id = f"{start}-{end}"
                    units.append((unit_id, segment))
                    current_pos = end

        return units

    def _create_shingles(self, text: str) -> set:
        """Convert text to set of n-gram shingles."""
        # Clean and normalize text
        cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', text.lower())
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        words = cleaned.split()
        
        if len(words) < self.shingle_size:
            return set()
        
        return set(' '.join(gram) for gram in ngrams(words, self.shingle_size))

    def _find_duplicates(self, units: List[Tuple[str, str]]) -> List[List[Tuple[str, str]]]:
        """Find groups of near-duplicate text units using LSH."""
        lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_perm)
        unit_hashes = {}
        
        # Build LSH index
        for i in range(0, len(units), self.batch_size):
            batch = units[i:i + self.batch_size]
            for unit_id, text in batch:
                shingles = self._create_shingles(text)
                if not shingles:
                    continue
                
                m = MinHash(num_perm=self.num_perm)
                for shingle in shingles:
                    m.update(shingle.encode('utf8'))
                
                lsh.insert(unit_id, m)
                unit_hashes[unit_id] = m

        # Find duplicate groups
        processed = set()
        groups = []
        
        for unit_id, text in units:
            if unit_id in processed or unit_id not in unit_hashes:
                continue
                
            m = unit_hashes[unit_id]
            candidates = lsh.query(m)
            candidates = [c for c in candidates if c != unit_id]
            
            if candidates:
                group = sorted([unit_id] + candidates)
                if len(group) > 1:
                    # Convert to (unit_id, text) tuples
                    group_with_text = [
                        (uid, next(text for u_id, text in units if u_id == uid))
                        for uid in group
                    ]
                    groups.append(group_with_text)
                    processed.update(group)

        return groups

    def _is_trivial_gap(self, gap_text: str) -> bool:
        """Check if gap between duplicates is trivial (OCR artifacts)."""
        trivial_pattern = r'^[\s\n\r.,;:!\-\[\]]*$'
        return bool(re.match(trivial_pattern, gap_text))

    def _identify_ocr_duplicates(self, duplicate_groups: List[List[Tuple[str, str]]], content: str) -> List[Tuple[int, int]]:
        """Identify OCR-induced duplicates (consecutive with trivial gaps)."""
        ocr_duplicates = []
        
        for group in duplicate_groups:
            # Sort by position
            group.sort(key=lambda x: int(x[0].split('-')[0]))
            
            # Check consecutive pairs for trivial gaps
            for i in range(len(group) - 1):
                current_span, _ = group[i]
                next_span, _ = group[i + 1]
                
                curr_start, curr_end = map(int, current_span.split("-"))
                next_start, next_end = map(int, next_span.split("-"))
                
                gap_text = content[curr_end:next_start]
                
                if self._is_trivial_gap(gap_text):
                    # Mark the second occurrence for removal
                    ocr_duplicates.append((next_start, next_end))
        
        # Remove duplicates and sort by position (reverse order for safe removal)
        ocr_duplicates = list(set(ocr_duplicates))
        ocr_duplicates.sort(key=lambda x: x[0], reverse=True)
        
        return ocr_duplicates

    def _remove_duplicates(self, content: str, duplicates: List[Tuple[int, int]]) -> str:
        """Remove duplicate segments from content."""
        new_content = content
        
        # Remove duplicates in reverse order to maintain positions
        for start, end in duplicates:
            # Verify the content matches expectations
            if start < len(new_content) and end <= len(new_content):
                new_content = new_content[:start] + new_content[end:]
        
        return new_content