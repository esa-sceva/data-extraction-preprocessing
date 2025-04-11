# Run inside the eve-data-extraction

python -m venv ../venv
source ../venv/bin/activate
apt update
apt install install -y \
    git \
    build-essential \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libglib2.0-0 \
    libgl1-mesa-glx

pip install -r ../data_extraction_pipeline/requirements.txt
