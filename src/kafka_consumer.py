# src/kafka_consumer.py
"""
Kafka Consumer — reads raw weather messages from 'weather-raw' topic,
transforms them, and loads into PostgreSQL and AWS S3.
"""

import json
import logging
import os
import signal
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from transformation import transform_weather_data
from storage import save_weather_data
from s3_storage import upload_to_s3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_RAW = 'weather-raw'
CONSUMER_GROUP = 'weather-pipeline-group'

# Graceful shutdown flag
_running = True


def handle_shutdown(signum, frame):
    global _running
    logger.info("🛑 Shutdown signal received — stopping consumer...")
    _running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def get_consumer():
    """Create and return a KafkaConsumer instance."""
    return KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
    )


def process_message(message_value):
    """
    Process a single Kafka message:
    1. Extract raw payload
    2. Transform with Pandas
    3. Load into PostgreSQL
    4. Upload raw JSON to AWS S3

    Parameters:
        message_value (dict): Deserialized Kafka message

    Returns:
        bool: True if successful
    """
    ingested_at = message_value.get('ingested_at')
    raw_data = message_value.get('payload')

    if not raw_data:
        logger.warning("⚠️ Empty payload in message, skipping")
        return False

    city = raw_data.get('name', 'unknown')
    logger.info(f"🔄 Processing message for {city} (ingested at {ingested_at})")

    # Transform
    df = transform_weather_data(raw_data)
    if df is None:
        logger.error(f"❌ Transformation failed for {city}")
        return False

    # Load to PostgreSQL
    db_success = save_weather_data(df)
    if db_success:
        logger.info(f"✅ Saved {city} data to PostgreSQL")
    else:
        logger.warning(f"⚠️ PostgreSQL save failed for {city}")

    # Upload raw JSON to S3
    s3_key = f"raw/{ingested_at[:10]}/{city}_{ingested_at}.json"
    s3_success = upload_to_s3(raw_data, s3_key)
    if s3_success:
        logger.info(f"✅ Uploaded {city} raw data to S3: {s3_key}")
    else:
        logger.warning(f"⚠️ S3 upload failed for {city}")

    return db_success


def run_consumer():
    """
    Main consumer loop — polls Kafka and processes messages until shutdown.
    """
    logger.info(f"🚀 Starting consumer on topic '{TOPIC_RAW}' (group: {CONSUMER_GROUP})")
    consumer = get_consumer()

    processed = 0
    errors = 0

    try:
        while _running:
            messages = consumer.poll(timeout_ms=1000)

            for topic_partition, records in messages.items():
                for record in records:
                    try:
                        success = process_message(record.value)
                        if success:
                            processed += 1
                        else:
                            errors += 1
                    except Exception as e:
                        logger.error(f"❌ Error processing message: {e}")
                        errors += 1

    finally:
        consumer.close()
        logger.info(f"🏁 Consumer stopped — processed: {processed}, errors: {errors}")


if __name__ == '__main__':
    run_consumer()
