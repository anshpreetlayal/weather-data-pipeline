# src/kafka_producer.py
"""
Kafka Producer — publishes raw weather API responses to the 'weather-raw' topic.
Sits between the Extract and Transform steps of the ETL pipeline.
"""

import json
import logging
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_RAW = 'weather-raw'


def get_producer():
    """
    Create and return a KafkaProducer instance.
    Serializes messages as JSON.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        retries=3,
        acks='all',  # wait for all replicas to acknowledge
    )


def publish_weather_data(raw_data_list, producer=None):
    """
    Publishes a list of raw weather API responses to the Kafka topic.

    Parameters:
        raw_data_list (list): Raw JSON responses from OpenWeatherMap
        producer (KafkaProducer): Optional existing producer instance

    Returns:
        int: Number of messages successfully published
    """
    close_after = producer is None
    if producer is None:
        producer = get_producer()

    published = 0

    for raw_data in raw_data_list:
        city = raw_data.get('name', 'unknown')
        # Wrap with ingestion timestamp for traceability
        message = {
            'ingested_at': datetime.utcnow().isoformat(),
            'source': 'OpenWeatherMap',
            'payload': raw_data,
        }

        try:
            future = producer.send(TOPIC_RAW, key=city, value=message)
            future.get(timeout=10)  # block until confirmed
            published += 1
            logger.info(f"✅ Published weather data for {city} to '{TOPIC_RAW}'")
        except KafkaError as e:
            logger.error(f"❌ Failed to publish data for {city}: {e}")

    producer.flush()

    if close_after:
        producer.close()

    logger.info(f"📨 Published {published}/{len(raw_data_list)} messages to Kafka")
    return published


if __name__ == '__main__':
    # Quick smoke test — fetch and publish one city
    from ingestion import fetch_weather_data
    data = fetch_weather_data('Toronto')
    if data:
        publish_weather_data([data])
