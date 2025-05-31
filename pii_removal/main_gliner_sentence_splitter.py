# !pip install -q gliner
# !pip install -q nltk

import nltk
nltk.download('punkt')
from gliner import GLiNER
import torch
import gc
import os
from tqdm.auto import tqdm

device = 'cuda' if torch.cuda.is_available() else 'cpu'

model = GLiNER.from_pretrained("E3-JSI/gliner-multi-pii-domains-v1").to(device)
labels = ["name", "organizations", "phone number", "email", "email address"]

def split_long_sentence(text, max_len=384, model=None):
    """
    Recursively split a sentence into chunks with <= max_len tokens.
    Returns a list of (chunk_text, start_offset, end_offset) tuples.
    """
    # tokenize to check length
    token_generator = model.data_processor.words_splitter(text)
    tokens_with_offsets = [(token, start, end) for token, start, end in token_generator]
    token_count = len(tokens_with_offsets)
    
    if token_count <= max_len:
        return [(text, tokens_with_offsets[0][1], tokens_with_offsets[-1][2])]

    # ff too long, split into roughly equal parts
    mid = len(text) // 2
    # a reasonable split point (e.g., nearest space or punctuation)
    while mid > 0 and text[mid] not in [' ', '.', ',', ';', '!', '?']:
        mid -= 1
    if mid == 0:  # no good split point found, force split
        mid = len(text) // 2

    left_text = text[:mid].strip()
    right_text = text[mid:].strip()
    
    # recursively split both parts
    chunks = []
    if left_text:
        chunks.extend(split_long_sentence(left_text, max_len, model))
    if right_text:
        chunks.extend(split_long_sentence(right_text, max_len, model))
    
    return chunks

def get_entities_from_long_text(model, text, labels, max_len=384, batch_size=4):
    sentences = nltk.sent_tokenize(text)
    print(f"Number of sentences: {len(sentences)}")

    sentence_spans = []
    current_pos = 0
    for sentence in sentences:
        # Find sentence offsets
        start = text.find(sentence, current_pos)
        end = start + len(sentence)
        
        token_generator = model.data_processor.words_splitter(sentence)
        tokens = [t for t in token_generator]
        token_count = len(tokens)
        
        if token_count <= max_len:
            sentence_spans.append((sentence, start, end))
        else:
            # Recursively split long sentence
            chunks = split_long_sentence(sentence, max_len, model)
            for chunk_text, chunk_start, chunk_end in chunks:
                chunk_start += start
                chunk_end += start
                sentence_spans.append((chunk_text, chunk_start, chunk_end))
        
        current_pos = end

    print(f"Number of chunks after splitting: {len(sentence_spans)}")

    all_entities = []
    for i in range(0, len(sentence_spans), batch_size):
        batch_sentences = [s[0] for s in sentence_spans[i:i + batch_size]]
        batch_spans = [(s[1], s[2]) for s in sentence_spans[i:i + batch_size]]
        
        entities = model.batch_predict_entities(batch_sentences, labels, threshold=0.5)
        
        for sentence_offset, entity_list in zip(batch_spans, entities):
            offset_start, _ = sentence_offset
            for entity in entity_list:
                entity["start"] += offset_start
                entity["end"] += offset_start
                all_entities.append(entity)

        torch.cuda.empty_cache()
        gc.collect()

    return all_entities

def replace_entities_with_labels(text, entities):
    # Sort entities by start index in descending order to avoid index shifting
    sorted_entities = sorted(entities, key=lambda x: x['start'], reverse=True)

    for entity in sorted_entities:
        label = entity['label'].upper()
        replacement = f'[{label}: {entity["text"]}]'
        text = text[:entity['start']] + replacement + text[entity['end']:]
    
    return text

input_dir = '/content/gdrive/MyDrive/pii_extraction_sample/original'
output_dir = '/content/gdrive/MyDrive/pii_extraction_sample/annotated_gliner_splitter_v2'

os.makedirs(output_dir, exist_ok=True)

for _f in tqdm(os.listdir(input_dir)):
    input_path = os.path.join(input_dir, _f)
    output_path = os.path.join(output_dir, f"{_f}")

    with open(input_path, 'r') as f:
        text = f.read()

        ents = get_entities_from_long_text(model, text, labels)

        modified_text = replace_entities_with_labels(text, ents)

        with open(output_path, 'w') as f:  
            f.write(modified_text)