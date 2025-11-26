"""This script does PII removal using GLINER model and redacts the extractions with the entity labels."""

# !pip install -q gliner
import os
from tqdm.auto import tqdm

from gliner import GLiNER
model = GLiNER.from_pretrained("E3-JSI/gliner-multi-pii-domains-v1").to('cuda')

labels = ["name", "organizations", "phone number", "email", "email address"]
input_dir = '/content/drive/MyDrive/pii_extraction_sample/original'
output_dir = '/content/drive/MyDrive/pii_extraction_sample/annotated_sentence_splits'

os.makedirs(output_dir, exist_ok=True)

for _f in tqdm(os.listdir(input_dir)):
      input_path = os.path.join(input_dir, _f)
      output_path = os.path.join(output_dir, f"{_f}")

      with open(input_path, 'r') as f:
          text = f.read()

      all_sentences = text.split(".")

      redacted_sentences = []

      for sentence in all_sentences:
          entities = model.predict_entities(sentence, labels, threshold=0.5)

          if not entities:
              redacted_sentences.append(sentence)
              continue

          # sort entities by start index to ensure clean replacements
          entities = sorted(entities, key=lambda e: e["start"])

          redacted = []
          last_idx = 0

          for entity in entities:
              start, end = entity['start'], entity['end']
              original_text = sentence[start:end]
              label = entity['label'].upper().replace(' ', '_')
              placeholder = f"[{label}: {original_text}]"

              redacted.append(sentence[last_idx:start])
              redacted.append(placeholder)
              last_idx = end

          redacted.append(sentence[last_idx:])
          redacted_sentences.append("".join(redacted))
      

      with open(output_path, 'w') as f:
        f.write("".join(redacted_sentences))