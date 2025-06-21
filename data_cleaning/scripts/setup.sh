#!/bin/bash

set -e 

apt-get update

apt-get install -y nano screen

apt-get install -y texlive-latex-base \
                   texlive-fonts-recommended \
                   texlive-fonts-extra \
                   texlive-latex-extra

apt-get install -y python3.12 python3.12-venv

python3.12 -m venv venv

echo "Setup complete. To activate the virtual environment, run: source venv/bin/activate"
