FROM python:3.11-slim

WORKDIR /app 

RUN pip install --no-cache-dir boto3 confluent-kafka

COPY kafka-2-minio.py .

CMD [ "python", "kafka-2-minio.py" ]