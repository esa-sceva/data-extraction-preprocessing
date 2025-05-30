# !pip install -q gliner

from gliner import GLiNER
import torch

device = 'cuda' if torch.cuda.is_available() else 'cpu'

model = GLiNER.from_pretrained("E3-JSI/gliner-multi-pii-domains-v1").to(device)
labels = ["name", "organizations", "phone number", "email", "email address"]

def get_spans_from_mapping(offset_mapping):
    return [(chunk[1][0], chunk[-2][1]) for chunk in offset_mapping]

def get_texts_from_spans(text, spans):
    return [text[start:end] for start, end in spans]

def get_entities_from_long_text(model, text, labels):
    transformer_tokenizer = model.data_processor.transformer_tokenizer
    max_len = model.config.max_len
    #max_len = 300

    encoded = transformer_tokenizer(
        text,
        return_overflowing_tokens = True,  # ensure to True to return the extra tokens > max len
        max_length = max_len,
        truncation = True,
        return_offsets_mapping = True  # get the mappings in order to cut the text into chunks afterwards
    )
    mapping = encoded["offset_mapping"]

    spans = get_spans_from_mapping(offset_mapping=mapping)
    texts = get_texts_from_spans(text=text, spans=spans)

    # print(len(texts[0]), len(texts[1]))

    print(len(texts))
    entities = model.batch_predict_entities(texts, labels, threshold=0.5) # batch preds, since if we want a fixed batch size.

    all_entities = []
    for span, entity_list in zip(spans, entities):
        offset, _ = span
        for entity in entity_list:
            entity["start"] += offset
            entity["end"] += offset
            all_entities.append(entity)

    return all_entities

def replace_entities_with_labels(text, entities):
    # Sort entities by start index in descending order , to avoid moving of indices
    sorted_entities = sorted(entities, key=lambda x: x['start'], reverse=True)

    for entity in sorted_entities:
        label = entity['label'].upper()
        replacement = f'[{label}: {entity["text"]}]'

        text = text[:entity['start']] + replacement + text[entity['end']:]

    return text

import os
from tqdm.auto import tqdm

input_dir = '/content/drive/MyDrive/pii_extraction_sample/original'
output_dir = '/content/drive/MyDrive/pii_extraction_sample/annotated_tokenized'

labels = ['name', 'organizations', 'phone number', 'email', 'email address']

os.makedirs(output_dir, exist_ok=True)

for _f in tqdm(os.listdir(input_dir)):
    input_path = os.path.join(input_dir, _f)
    output_path = os.path.join(output_dir, f"{_f}")

    with open(input_path, 'r') as f:
        text = f.read()

        ents = get_entities_from_long_text(model, text, labels = labels)

        modified_text = replace_entities_with_labels(text, ents)

        with open(output_path, 'w') as f:
            f.write(modified_text)