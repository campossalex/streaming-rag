FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY seed_milvus.py corpus_extra.json ./
CMD ["python", "seed_milvus.py"]
