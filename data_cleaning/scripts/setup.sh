#!/bin/bash

set -e

apt-get update

apt-get install -y screen nano

python3 -m venv venv

echo "Setup complete. To activate the virtual environment, run: source venv/bin/activate"