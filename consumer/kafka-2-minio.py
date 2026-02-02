from ensurepip import bootstrap
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

print(KAFKA_CONFIG)

MINIO_CONFIG= {
    'endpoint_url': 'http://minio:9000',
    'aws_access_key_id': 'admin',
    'aws_secret_access_key': 'admin123',
    'region_name': 'us-east-1'
}


consumer= Consumer(KAFKA_CONFIG)
s3_client = boto3.client("s3", **MINIO_CONFIG)
bucket_name= 'banker_bucket'


def run_consumer():
    response = s3_client.list_buckets()
    print(response["Buckets"])

    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except Exception:
        pass # Bucket likely already exists

    consumer.subscribe(['banking_dev.public.acc_accounts'])

    print('listening for events')

    try:
        while True:
            msg= consumer.poll(100.0)
            if msg is None:
                continue

            if msg.error():
                print(f"consumer error : {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            
            # Create a unique filename based on timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            file_name = f"transactions/raw_transaction_{timestamp}.json"

            # Upload to MinIO
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            print(f"Stored: {file_name}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()


