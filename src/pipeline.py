# src/pipeline.py
"""
Main pipeline entry point.
Runs the full ETL cycle once:
  Extract → Kafka → Transform → Load (PostgreSQL + S3)

Use this for standalone runs or testing outside Airflow.
"""

import logging
import sys
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger(__name__)


def run_pipeline():
    logger.info("=" * 60)
    logger.info("🚀 Weather ETL Pipeline — starting run")
    logger.info(f"   Timestamp: {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)

    # ── 1. EXTRACT ───────────────────────────────────────────
    logger.info("\n📡 Step 1: Extracting data from OpenWeatherMap API...")
    from ingestion import fetch_all_cities
    raw_data_list = fetch_all_cities()

    if not raw_data_list:
        logger.error("❌ No data fetched — aborting pipeline")
        sys.exit(1)

    logger.info(f"✅ Fetched data for {len(raw_data_list)} cities")

    # ── 2. PUBLISH TO KAFKA ──────────────────────────────────
    logger.info("\n📨 Step 2: Publishing raw data to Kafka...")
    from kafka_producer import publish_weather_data
    published = publish_weather_data(raw_data_list)
    logger.info(f"✅ Published {published} messages to Kafka topic 'weather-raw'")

    # ── 3. TRANSFORM ─────────────────────────────────────────
    logger.info("\n🔄 Step 3: Transforming raw data...")
    from transformation import transform_multiple_cities, add_calculated_fields
    df = transform_multiple_cities(raw_data_list)

    if df is None or df.empty:
        logger.error("❌ Transformation produced no data — aborting")
        sys.exit(1)

    df = add_calculated_fields(df)
    logger.info(f"✅ Transformed {len(df)} records")

    # ── 4. LOAD → PostgreSQL ─────────────────────────────────
    logger.info("\n🗄️  Step 4: Loading data into PostgreSQL...")
    from storage import save_weather_data
    db_success = save_weather_data(df)

    if db_success:
        logger.info("✅ Data saved to PostgreSQL")
    else:
        logger.warning("⚠️ PostgreSQL save failed — continuing to S3 step")

    # ── 5. UPLOAD → S3 ───────────────────────────────────────
    logger.info("\n☁️  Step 5: Uploading raw data to AWS S3...")
    from s3_storage import upload_to_s3, upload_dataframe_to_s3

    today = datetime.utcnow().strftime('%Y-%m-%d')
    run_ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

    # Upload each city's raw JSON
    for raw in raw_data_list:
        city = raw.get('name', 'unknown')
        s3_key = f"raw/{today}/{city}_{run_ts}.json"
        upload_to_s3(raw, s3_key)

    # Upload transformed CSV
    csv_key = f"transformed/{today}/all_cities_{run_ts}.csv"
    upload_dataframe_to_s3(df, csv_key)

    # ── DONE ─────────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("🏁 Pipeline run complete")
    logger.info(f"   Cities processed: {len(raw_data_list)}")
    logger.info(f"   Records loaded:   {len(df)}")
    logger.info(f"   PostgreSQL:       {'✅' if db_success else '⚠️ failed'}")
    logger.info("=" * 60)


if __name__ == '__main__':
    run_pipeline()
