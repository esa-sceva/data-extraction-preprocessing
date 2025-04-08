import os
from pathlib import Path

def setup_directories(base_path, sub_folder=None):
    """Set up necessary directories for storing extracted files."""
    os.makedirs(base_path, exist_ok=True)
    if sub_folder:
        os.makedirs(f"{base_path}/{sub_folder}", exist_ok=True)

def discover_subdirectories(base_dir):
    """Discover all subdirectories in the base directory."""
    base_path = Path(base_dir)
    subdirs = []
    try:
        for path in base_path.iterdir():
            if path.is_dir():
                subdirs.append(path.name)
        if not subdirs:
            subdirs = [base_path.name]
    except Exception as e:
        print(f"Error discovering subdirectories: {str(e)}")
    return subdirs

def get_files_in_directory(directory_path):
    """Get all files in a directory recursively."""
    directory = Path(directory_path)
    file_list = list(directory.glob('**/*'))
    return [f for f in file_list if f.is_file()]