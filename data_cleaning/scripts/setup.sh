#!/bin/bash

set -e

apt-get update

apt-get install -y screen nano

python3 -m venv venv

source venv/bin/activate

pip install awscli boto3 colorama datasketch nltk rapidfuzz tqdm

echo "Setup complete with packages"