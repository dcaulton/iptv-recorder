# Dockerfile
FROM python:3.12-slim

# Install git (required for git+ in requirements.txt)
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY iptv_recorder.py .

CMD ["python", "iptv_recorder.py"]
