import json
import gzip
import time
from botocore.config import Config
import boto3
import json
from confluent_kafka import Consumer
from datetime import datetime



KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'minio-sink-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'security.protocol': 'PLAINTEXT',
}

print(f"KAFKA_CONFIG: {KAFKA_CONFIG}");

MINIO_CONFIG= {
    'endpoint_url': 'http://minio:9000',
    'aws_access_key_id': 'admin',
    'aws_secret_access_key': 'admin123',
    'region_name': 'us-east-1'
}

print(f"MINIO_CONFIG: {MINIO_CONFIG}")

consumer= Consumer(KAFKA_CONFIG)
print("consumer is created, connected to kafka broker")

s3_client = boto3.client("s3", **MINIO_CONFIG, config=Config(connect_timeout=5, read_timeout=5))
print(f"s3_client is created, connected to minio server")

bucket_name='banker-bucket-de-project-dev-1'
print(f"bucket_name is created, connected to minio server")




# ----------------------------
# Micro-batch config
# ----------------------------
BATCH_SIZE = 5000          # records
FLUSH_INTERVAL = 10        # seconds
batch = []
last_flush = time.time()

# ----------------------------
# Flush function
# ----------------------------
def flush_batch():
    global batch, last_flush

    if not batch:
        return

    filename = (
        f"transactions/"
        f"batch_{datetime.strftime('%Y%m%d_%H%M%S_%f')}_"
        f"{datetime.datetime.now(datetime.UTC)}.json.gz"
    )

    payload = "\n".join(json.dumps(r) for r in batch).encode("utf-8")
    compressed = gzip.compress(payload)

    s3_client.put_object(
        bucket_name,
        filename,
        data=compressed,
        length=len(compressed),
        content_type="application/gzip"
    )

    print(f"Flushed {len(batch)} records → {filename}")

    batch = []
    last_flush = time.time()

# ----------------------------
# Kafka consumer loop
# ----------------------------
def run_consumer():
    consumer.subscribe(['banking_dev.public.acc_transactions'])
    print("Listening for events...")

    try:
        while True:
            msg = consumer.poll(100.0)

            if msg is None:
                # time-based flush
                if time.time() - last_flush >= FLUSH_INTERVAL:
                    flush_batch()
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            record = json.loads(msg.value().decode("utf-8"))
            batch.append(record)

            # size-based flush
            if len(batch) >= BATCH_SIZE:
                flush_batch()

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        flush_batch()   # flush remaining records
        consumer.close()

if __name__ == "__main__":
    run_consumer()