# REFERENCE - https://github.com/ekzhu/datasketch

from datasketch import MinHash, MinHashLSH
import os
from nltk import ngrams
import time
from tqdm.auto import tqdm

'''
Adjust NUM_PERM: Higher values increase accuracy but use more memory.
Adjust THRESHOLD: Higher values find closer duplicates but may miss some.
Adjust SHINGLE_SIZE: Larger shingles are more specific but increase computation.
'''

SHINGLE_SIZE = 3  # Size of n-grams
NUM_PERM = 128    # Number of permutations for MinHash
THRESHOLD = 0.8   # Jaccard similarity threshold for near-duplicates
BATCH_SIZE = 1000 # Process files in batches to manage memory


class LSH:
    def __init__(self, FILE_DIR, SHINGLE_SIZE, NUM_PERM, THRESHOLD, BATCH_SIZE):
        """
        FILE_DIR     - directory of the main folder
        SHINGLE_SIZE - Size of n-grams
        NUM_PERM     - Number of permutations for MinHash
        THRESHOLD    - Jaccard similarity threshold for near-duplicates
        BATCH_SIZE   - Process files in batches to manage memory
        """
        self.FILE_DIR = FILE_DIR
        self.SHINGLE_SIZE = SHINGLE_SIZE
        self.NUM_PERM = NUM_PERM
        self.THRESHOLD = THRESHOLD
        self.BATCH_SIZE = BATCH_SIZE
        self.file_paths = []
        self.file_hashes = {}
        self.duplicates = []

        for (root, _, files) in os.walk(self.FILE_DIR):
            for file in files:
                self.file_paths.append(os.path.join(root, file))
        print(f"Total Files : {len(self.file_paths)}")

    def create_shingles(self, text):
        words = text.lower().split()
        return set(' '.join(gram) for gram in ngrams(words, self.SHINGLE_SIZE))
    
    def do_lsh(self):
        lsh = MinHashLSH(threshold=self.THRESHOLD, num_perm=self.NUM_PERM)

        start_time = time.time()

        for i, file_path in tqdm(enumerate(self.file_paths), total = len(self.file_paths)):
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
                shingles = self.create_shingles(text)
                
                m = MinHash(num_perm=self.NUM_PERM)
                for shingle in shingles:
                    m.update(shingle.encode('utf8'))
                
                lsh.insert(file_path, m)
                self.file_hashes[file_path] = m

            # if (i + 1) % self.BATCH_SIZE == 0:
            #     print(f"Processed {i + 1} files...")

        print(f"Total processing time: {time.time() - start_time:.2f} seconds")
        return lsh
    
    def get_duplicates(self):
        lsh = self.do_lsh()
        processed = set()
        self.duplicates = []
        
        for file_path in self.file_paths:
            if file_path in processed:
                continue
            m = self.file_hashes[file_path]
            candidates = lsh.query(m)
            # Exclude the file itself and ensure there are other similar files
            candidates = [c for c in candidates if c != file_path]
            if candidates:  # If there are near-duplicates
                # Create a group including the current file and its near-duplicates
                group = [file_path] + candidates
                # Sort the group to ensure consistent ordering
                group = sorted(group)
                # Only add the group if it hasn't been added before
                if group not in self.duplicates:
                    self.duplicates.append(group)
                # Mark all files in the group as processed
                processed.update(group)

        print(f"Found {len(self.duplicates)} groups of near-duplicates:")
        return self.duplicates


lsh = LSH('data', 3, 128, 0.8, 2)
dupes = lsh.get_duplicates()

with open("dupes.txt", 'w') as f:
    f.write(str(dupes))
#print(dupes)