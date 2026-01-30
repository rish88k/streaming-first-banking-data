FROM python:3.9-slim

WORKDIR /app 

RUN pip install --no-cache-dir boto3 confluent-kafka

COPY . .

CMD [ "python", "kafka-2-minio.py" ]