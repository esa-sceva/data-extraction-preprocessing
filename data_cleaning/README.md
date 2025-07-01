### This folder contains scripts for basic cleaning of data.

1. Run `main.py` file to remove basic artifacts and regular old data cleaning.

(Remember to run PII seperately since its not compactible with multiprocessing)

Sample Usage - 

```
python main.py --base_dir ../data/arxiv --num_processes 8 --save_to_s3 --debug
```


Points to remember

1. Remove the two files from esa folder .
   - b1dcff21-baa1-4c8a-a9c1-77623ecf21cb.html
   - 091964cb-4817-48bd-b33d-5be8727657b9.html
   
(check the file format and content for reason)