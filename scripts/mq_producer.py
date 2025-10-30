import os
import json
import time
import pika
import feedparser
from dotenv import load_dotenv

# -----------------------------
# Load .env
# -----------------------------
load_dotenv()

# -----------------------------
# RabbitMQ Configuration
# -----------------------------
RABBITMQ_NODE = 'MQ Server/Cluster name'
PORT = 5672
QUEUE_NAME = 'RSS_FEED_QUEUE'
HEARTBEAT = 60                    # heartbeat in seconds
BLOCKED_TIMEOUT = 300             # blocked connection timeout

# -----------------------------
# RSS Feed URLs
# -----------------------------
RSS_FEEDS = [
    "https://openai.com/blog/rss/",
    "https://medium.com/feed/topic/artificial-intelligence",
    "https://ai.googleblog.com/feeds/posts/default",
    "https://techcrunch.com/feed/",
    "https://www.bbc.co.uk/feeds/rss/world.xml",
    "https://www.theverge.com/rss/index.xml",
    "https://www.wired.com/feed/rss",
    "https://thenextweb.com/feed/",
    "https://arxiv.org/rss/cs.AI",
    "https://arxiv.org/rss/cs.LG",
    "https://arxiv.org/rss/cs.CV",
    "https://towardsdatascience.com/feed",
    "https://deepmind.com/blog/rss.xml",
    "https://venturebeat.com/category/ai/feed/",
    "https://www.forbes.com/ai/feed2/",
    "https://www.techradar.com/rss",
    "https://analyticsindiamag.com/feed/",
    "https://www.kdnuggets.com/feed",
    "https://www.infoq.com/ai-ml/news/rss",
    "https://machinelearningmastery.com/blog/feed/"
]

# -----------------------------
# File to track already processed links
# -----------------------------
PROCESSED_FILE = "processed_rss_links.json"

def load_processed_links():
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_processed_links(links):
    with open(PROCESSED_FILE, "w") as f:
        json.dump(list(links), f)

# -----------------------------
# RabbitMQ Connection with reconnection
# -----------------------------
def connect_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_NODE,
                    port=PORT,
                    heartbeat=HEARTBEAT,
                    blocked_connection_timeout=BLOCKED_TIMEOUT
                )
            )
            print(f"✅ Connected to RabbitMQ node: {RABBITMQ_NODE}:{PORT}")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("❌ Connection failed, retrying in 5 seconds...")
            time.sleep(5)

# -----------------------------
# Fetch RSS Feeds
# -----------------------------
def fetch_rss_entries():
    entries = []
    for url in RSS_FEEDS:
        feed = feedparser.parse(url)
        for item in feed.entries:
            entries.append({
                "title": item.get("title"),
                "summary": item.get("summary"),
                "link": item.get("link"),
                "published": item.get("published"),
                "source": feed.feed.get("title", "Unknown")
            })
    return entries

# -----------------------------
# Send message with reconnection retry
# -----------------------------
def send_message(channel, message, connection):
    try:
        channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=json.dumps(message))
        print("✅ Sent:", message["rss_entry"]["title"])
    except (pika.exceptions.StreamLostError, pika.exceptions.AMQPConnectionError):
        print("❌ RabbitMQ connection lost. Reconnecting...")
        connection = connect_rabbitmq()
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True, arguments={'x-queue-type': 'stream'})
        channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=json.dumps(message))
        print("🔄 Reconnected and sent message:", message["rss_entry"]["title"])
    return channel, connection

# -----------------------------
# Producer Logic
# -----------------------------
def run_rss_producer():
    processed_links = load_processed_links()
    connection = connect_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True, arguments={'x-queue-type': 'stream'})

    msg_id = 1
    print(" [*] RSS Producer running. Press CTRL+C to stop.")

    try:
        while True:
            entries = fetch_rss_entries()
            new_entries = [e for e in entries if e["link"] not in processed_links]

            if new_entries:
                for entry in new_entries:
                    message = {"msg_id": str(msg_id), "rss_entry": entry}
                    channel, connection = send_message(channel, message, connection)
                    processed_links.add(entry["link"])
                    msg_id += 1

                # Save processed links to file
                save_processed_links(processed_links)
            else:
                print("⚠️ No new RSS entries found. Waiting for next interval...")

            # Wait 15 minutes before next fetch
            time.sleep(15 * 60)

    except KeyboardInterrupt:
        print(" [*] Producer stopped by user.")
    finally:
        save_processed_links(processed_links)
        if connection.is_open:
            connection.close()
            print("✅ RabbitMQ connection closed safely.")

# -----------------------------
# Run Script
# -----------------------------
if __name__ == "__main__":
    run_rss_producer()
