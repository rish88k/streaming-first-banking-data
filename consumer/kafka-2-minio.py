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


def run_consumer():
    response = s3_client.list_buckets()
    print(response["Buckets"])

    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except s3_client.exceptions.BucketAlreadyExists:
        print("bucket already exists")
        pass
    except s3_client.exceptions.ClientError as e:
        print(f"bucket client error: {e}")
        pass
    except Exception as e:
        print(f"bucket creation error: {e}")
        pass

    consumer.subscribe(['banking_dev.public.acc_transactions'])

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


