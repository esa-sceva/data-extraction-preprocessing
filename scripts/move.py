import subprocess

with open('rerun_files.txt', 'r') as f:
    files = f.readlines()

for _file in files:
    provider, file = _file.split('/')
    bucket = f"s3://llm4eo-s3/raw_data_unduplicated/{provider}"
    destination = f"data/{provider}"
    s3_path = f"{bucket}/{file}"
    local_path = f"{destination}/{file}"
    
    subprocess.run(["aws", "s3", "cp", s3_path, local_path], check=True)