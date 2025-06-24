import shutil
from pathlib import Path

source_root = Path("data")
destination_root = Path("data_new")
path_list_file = Path("sampled_5k.txt")
destination_root.mkdir(parents=True, exist_ok=True)

with open(path_list_file) as f:
    for line in f:
        relative_path = line.strip()
        if not relative_path:
            continue

        src_path = source_root / relative_path
        dst_path = destination_root / relative_path

        dst_path.parent.mkdir(parents = True, exist_ok = True) 
        shutil.copy2(src_path, dst_path)
        print(f"Copied: {src_path} to {dst_path}")
