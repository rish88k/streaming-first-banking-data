FROM python:3.11-slim

WORKDIR /app 

# Added python3-dev for better C-extension support
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir boto3 confluent-kafka

ENV PYTHONUNBUFFERED=1

COPY micro-batching.py  .

CMD [ "python", "-u", "micro-batching.py" ]