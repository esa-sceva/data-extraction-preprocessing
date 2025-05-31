### This folder has scripts for PII removal using GLINER model.

1. `main_sentence_splits.py` splits text as sentences and then does the PII.

UserWarning: Sentence of length 1005 has been truncated to 768
warnings.warn(f"Sentence of length {len(tokens)} has been truncated to {max_len}")


2. `main_tokenized.py` splits text and calculates offsets using the in-built tokenizer.
(deberta)

3. `main_gliner_splitter.py` split into tokens using the inbuilt words_splitter. Poor perf since the tokens generated do not consider spaces and breaks the notion of sentences.

4. `main_gliner_sentence_splitter.py` first split into sentences, then recursively keep splitting until the len < max-tokens(use words_splitter to check len or else wrong results), recursive split if a nearby landmark like eos or else split in half. 
 
