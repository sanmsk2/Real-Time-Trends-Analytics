import os
import json
import time
from datetime import datetime, timedelta
import pika
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

# -----------------------------
# Load .env
# -----------------------------
load_dotenv()

# -----------------------------
# RabbitMQ Configuration
# -----------------------------
RABBITMQ_NODE = 'MQ Server/Cluster Name'
PORT = 5672
QUEUE_NAME = 'RSS_FEED_QUEUE'

# -----------------------------
# GCP Storage Configuration
# -----------------------------
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")  # Corrected: use a valid .env variable
GCS_FOLDER = "raw_rss_feed"  # root folder in bucket
SERVICE_ACCOUNT_FILE = os.getenv("GCP_SERVICE_ACCOUNT_FILE")  # Corrected: store path in .env

# -----------------------------
# Consumer Config
# -----------------------------
BATCH_SIZE = 100             # number of messages per batch
FLUSH_INTERVAL = 15 * 60    # flush every 15 minutes (seconds)

# -----------------------------
# Connect to RabbitMQ
# -----------------------------
def connect_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_NODE, port=PORT)
            )
            print(f"✅ Connected to RabbitMQ node: {RABBITMQ_NODE}:{PORT}")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("❌ Connection failed, retrying in 5 seconds...")
            time.sleep(5)

# -----------------------------
# Upload JSON to GCS (daily folder)
# -----------------------------
def upload_to_gcs(file_name, data):
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(GCS_BUCKET_NAME)

    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    blob_name = f"{GCS_FOLDER}/{today_str}/{file_name}"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type="application/json")
    print(f"✅ Uploaded {file_name} to GCS/{today_str}/ (records: {len(json.loads(data))})")

# -----------------------------
# Consumer Logic
# -----------------------------
def run_consumer():
    connection = connect_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        arguments={'x-queue-type': 'stream'}
    )

    part_number = 1
    current_date = datetime.utcnow().date()
    batch = []
    last_flush_time = datetime.utcnow()

    # Flush batch to GCS
    def process_batch():
        nonlocal batch, part_number
        if batch:
            utc_timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            file_name = f"rss_batch_{utc_timestamp}_part{part_number}.json"
            upload_to_gcs(file_name, json.dumps(batch, indent=2))
            batch.clear()
            part_number += 1

    def reset_part_number():
        nonlocal part_number
        part_number = 1
        print("🔄 Part number reset to 1 for new day.")

    # Callback for each message
    def callback(ch, method, properties, body):
        nonlocal batch, current_date
        data = json.loads(body)
        batch.append(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # Flush if batch full
        if len(batch) >= BATCH_SIZE:
            process_batch()

        # Reset part number if day changed
        today = datetime.utcnow().date()
        if today != current_date:
            current_date = today
            reset_part_number()

    print(" [*] RSS Consumer running. Press CTRL+C to stop.")
    channel.basic_qos(prefetch_count=100)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    try:
        while True:
            # Process messages continuously
            channel.connection.process_data_events(time_limit=1)

            # Flush based on flush interval (every 15 minutes)
            now = datetime.utcnow()
            if (now - last_flush_time).total_seconds() >= FLUSH_INTERVAL:
                print("⏱ Flush interval reached. Flushing batch to GCS...")
                process_batch()
                last_flush_time = now

    except KeyboardInterrupt:
        print(" [*] Consumer stopped by user.")
        process_batch()  # flush remaining messages
    finally:
        if connection.is_open:
            connection.close()
            print("✅ RabbitMQ connection closed safely.")

# -----------------------------
# Run Script
# -----------------------------
if __name__ == "__main__":
    run_consumer()
