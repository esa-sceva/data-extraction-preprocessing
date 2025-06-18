'''This script is used to get total words and token estimate across a folder for md files.'''

import argparse
from tqdm.auto import tqdm
import os
import logging
from multiprocessing import Pool, cpu_count
from collections import defaultdict

logging.basicConfig(filename='stats.log', level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')

def process_file(file_path):
    """Get stats for the files"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()

        num_words = len(text.split())
        subfolder = os.path.dirname(file_path).split(os.sep)[-1]
        
        # enable for extra information

        # logging.info(f"File: {os.path.basename(file_path)}")
        # logging.info(f"Subfolder: {subfolder}")
        # logging.info(f"Number of Words: {num_words}")
        # logging.info(f"Number of Tokens: {num_words * 1.73}")
        # logging.info("-" * 50)
        
        return subfolder, num_words, num_words * 1.73, 1 # 1 is denoting one count, basically one file
    
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {str(e)}")
        return None, 0, 0

def linear_processing(files):
    """Process files linearly."""
    stats = defaultdict(lambda: {'words': 0, 'tokens': 0, 'count': 0}) # use default dict with lambda so when key not present, it auto-assigns this default value
    for file_path in tqdm(files, desc="Processing files"):
        subfolder, words, tokens, count = process_file(file_path)
        if subfolder:
            stats[subfolder]['words'] += words
            stats[subfolder]['tokens'] += tokens
            stats[subfolder]['count'] += count
    return stats

def multi_processing(files):
    """Process files using multiprocessing."""
    stats = defaultdict(lambda: {'words': 0, 'tokens': 0, 'count': 0})
    num_processes = max(1, cpu_count() - 1)
    
    with Pool(processes = num_processes) as pool:
        results = list(tqdm(
            pool.imap(process_file, files),
            total=len(files),
            desc="Processing files"
        ))
        
    for subfolder, words, tokens, count in results:
        if subfolder:
            stats[subfolder]['words'] += words
            stats[subfolder]['tokens'] += tokens
            stats[subfolder]['count'] += count
    
    return stats

def main():
    parser = argparse.ArgumentParser(description="Get statistics.")
    parser.add_argument('--multi', action='store_true', help="Use multiprocessing for parallel execution")
    args = parser.parse_args()

    md_files = []
    for root, _, files in os.walk('../../data'):
        for filename in files:
            if filename.lower().endswith('.md'):
                md_files.append(os.path.join(root, filename))

    if not md_files:
        logging.info("No MD files found.")
        print("No MD files found.")
        return

    if args.multi:
        print("Running in multiprocessing mode...")
        stats = multi_processing(md_files)
    else:
        print("Running in linear mode...")
        stats = linear_processing(md_files)

    if not stats:
        logging.info("No MDs were processed.")
        print("No MDs were processed.")
        return

    # Calculate totals
    total_words = sum(data['words'] for data in stats.values())
    total_tokens = sum(data['tokens'] for data in stats.values())
    total_files = sum(data['count'] for data in stats.values())
    
    # Log and print subfolder-wise statistics
    logging.info("\nSubfolder-wise Statistics:")
    print("\nSubfolder-wise Statistics:")
    print("-" * 50)
    
    for subfolder, data in stats.items():
        avg_words = data['words'] / data['count'] if data['count'] > 0 else 0
        avg_tokens = data['tokens'] / data['count'] if data['count'] > 0 else 0
        logging.info(f"Subfolder: {subfolder}")
        logging.info(f"  Files processed: {data['count']}")
        logging.info(f"  Total Words: {data['words']}")
        logging.info(f"  Average Words per File: {avg_words:.2f}")
        logging.info(f"  Total Tokens: {data['tokens']}")
        logging.info(f"  Average Tokens per File: {avg_tokens:.2f}")
        logging.info("-" * 30)
        
        print(f"Subfolder: {subfolder}")
        print(f"  Files processed: {data['count']}")
        print(f"  Total Words: {data['words']}")
        print(f"  Total Tokens: {data['tokens']}")
        print("-" * 30)
    
    # Log and print overall statistics
    overall_avg_words = total_words / total_files if total_files > 0 else 0
    overall_avg_tokens = total_tokens / total_files if total_files > 0 else 0
    logging.info("\nOverall Statistics:")
    logging.info(f"Total Files processed: {total_files}")
    logging.info(f"Total Words: {total_words}")
    logging.info(f"Average Words per File: {overall_avg_words:.2f}")
    logging.info(f"Total Tokens: {total_tokens}")
    logging.info(f"Average Tokens per File: {overall_avg_tokens:.2f}")
    
    print("\nOverall Statistics:")
    print(f"Total Files processed: {total_files}")
    print(f"Total Words: {total_words}")
    print(f"Total Tokens: {total_tokens}")

if __name__ == "__main__":
    main()