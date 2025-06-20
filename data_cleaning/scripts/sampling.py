from random import sample

with open(r"E:\pi_school\eve-esa\eve-data-extraction\data_cleaning\all_files.txt", 'r', encoding = 'utf-8') as file:
    content = file.readlines()

file_names = []

for _f in content:
    file_names.append('/'.join(_f.split(' ')[-1].split('/')[1:]).replace('\n', ''))

assert len(file_names) == len(content)


sampled = sample(file_names, 5000)

print(len(sampled))

with open("sampled_5k.txt", 'w') as f:
    for _f in sampled:
        f.write(_f)
        f.write('\n')