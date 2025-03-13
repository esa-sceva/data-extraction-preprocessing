def save_to_markdown(text, path):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    
    print(f"saved to {path}")