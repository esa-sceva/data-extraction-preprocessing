# !pip install -q gliner

from gliner import GLiNER
import torch
import gc
import os
from tqdm.auto import tqdm

device = 'cuda' if torch.cuda.is_available() else 'cpu'

model = GLiNER.from_pretrained("E3-JSI/gliner-multi-pii-domains-v1").to(device)
labels = ["name", "organizations", "phone number", "email", "email address"]

def get_entities_from_long_text(model, text, labels, max_len=384, batch_size=4):
    # Use words_splitter for tokenization
    token_generator = model.data_processor.words_splitter(text)
    tokens_with_offsets = [(token, start, end) for token, start, end in token_generator]
    
    total_tokens = len(tokens_with_offsets)
    print(f"Total tokens: {total_tokens}")

    chunks = []
    spans = []
    current_chunk = []
    current_token_count = 0
    current_start = tokens_with_offsets[0][1] if tokens_with_offsets else 0

    for token, start, end in tokens_with_offsets:
        current_chunk.append(token)
        current_token_count += 1

        if current_token_count >= max_len or (token, start, end) == tokens_with_offsets[-1]:
            chunk_text = "".join(current_chunk)
            chunk_end = end  
            chunks.append(chunk_text)
            spans.append((current_start, chunk_end))
            

            current_chunk = []
            current_start = end
            current_token_count = 0

    print(f"Number of chunks: {len(chunks)}")


    all_entities = []
    for i in range(0, len(chunks), batch_size):
        batch_texts = chunks[i:i + batch_size]
        batch_spans = spans[i:i + batch_size]
        

        entities = model.batch_predict_entities(batch_texts, labels, threshold=0.5)
        

        for span, entity_list in zip(batch_spans, entities):
            offset, _ = span
            for entity in entity_list:
                entity["start"] += offset
                entity["end"] += offset
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

input_dir = '/content/drive/MyDrive/pii_extraction_sample/original'
output_dir = '/content/drive/MyDrive/pii_extraction_sample/annotated_gliner_splitter'

os.makedirs(output_dir, exist_ok=True)

for _f in tqdm(os.listdir(input_dir)):
    input_path = os.path.join(input_dir, _f)
    output_path = os.path.join(output_dir, f"{_f}")

    with open(input_path, 'r') as f:
        text = f.read()

        # Get entities using the modified function
        ents = get_entities_from_long_text(model, text, labels, max_len=384)

        # Replace entities in text
        modified_text = replace_entities_with_labels(text, ents)

        # Write output
        with open(output_path, 'w') as f:
            f.write(modified_text)