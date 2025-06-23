### This folder contains scripts for basic cleaning of data.

1. Run `main.py` file to remove basic artifacts and regular old data cleaning.

(Remember to run PII seperately since its not compactible with multiprocessing)

Sample Usage - 

```
python main.py --base_dir ../data/arxiv --num_processes 8 --save-to-s3 --debug
```
