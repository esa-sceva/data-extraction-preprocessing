FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libglib2.0-0 \
    libgl1-mesa-glx \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir nougat-ocr
RUN pip install --no-cache-dir "nougat-ocr[api]"
RUN pip install --no-cache-dir fastapi uvicorn


EXPOSE 8503

CMD ["nougat_api", "--host", "0.0.0.0", "--port", "8503"]