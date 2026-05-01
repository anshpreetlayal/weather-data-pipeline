# dags/weather_pipeline_dag.py
"""
Airflow DAG — Weather Data Pipeline

Schedule: every 5 minutes
Flow:
    fetch_weather → publish_to_kafka → consume_and_load

The DAG triggers the producer (which fetches from OpenWeatherMap and
publishes to Kafka). A separate long-running consumer service handles
the transform + load steps. This DAG manages the extract + publish side.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.insert(0, '/opt/airflow/src')

# ── Default args ─────────────────────────────────────────────
default_args = {
    'owner': 'anshpreet',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# ── DAG definition ────────────────────────────────────────────
dag = DAG(
    dag_id='weather_etl_pipeline',
    description='Fetch weather data, publish to Kafka, load to PostgreSQL and S3',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['weather', 'etl', 'kafka', 's3'],
)


# ── Task functions ────────────────────────────────────────────

def task_fetch_and_publish(**context):
    """
    Extract: fetch weather data for all cities and publish raw
    JSON to the Kafka 'weather-raw' topic.
    """
    from ingestion import fetch_all_cities
    from kafka_producer import publish_weather_data

    raw_data_list = fetch_all_cities()

    if not raw_data_list:
        raise ValueError("❌ No weather data fetched — check API key and connectivity")

    published = publish_weather_data(raw_data_list)

    # Push to XCom so downstream tasks can read it
    context['ti'].xcom_push(key='cities_fetched', value=len(raw_data_list))
    context['ti'].xcom_push(key='messages_published', value=published)

    print(f"✅ Fetched {len(raw_data_list)} cities, published {published} messages to Kafka")
    return published


def task_upload_transformed_to_s3(**context):
    """
    After consumer has loaded data to PostgreSQL, pull the latest
    records and back them up to S3 as CSV (data lake layer).
    """
    from storage import WeatherDatabase
    from s3_storage import upload_dataframe_to_s3
    from datetime import datetime

    db = WeatherDatabase()
    if not db.connect():
        raise ConnectionError("❌ Could not connect to PostgreSQL")

    today = datetime.utcnow().strftime('%Y-%m-%d')
    run_ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

    all_uploaded = True
    cities = ['Toronto', 'Montreal', 'Vancouver']

    for city in cities:
        df = db.get_historical_data(city, days=1)
        if df is not None and not df.empty:
            s3_key = f"transformed/{today}/{city}_{run_ts}.csv"
            success = upload_dataframe_to_s3(df, s3_key)
            if not success:
                all_uploaded = False
        else:
            print(f"⚠️ No data found for {city} in last 24h")

    db.disconnect()

    if not all_uploaded:
        raise RuntimeError("❌ One or more S3 uploads failed")

    print(f"✅ Transformed CSVs uploaded to S3 for run {run_ts}")


def task_log_pipeline_stats(**context):
    """Log pipeline run statistics for monitoring."""
    ti = context['ti']
    cities = ti.xcom_pull(task_ids='fetch_and_publish', key='cities_fetched') or 0
    published = ti.xcom_pull(task_ids='fetch_and_publish', key='messages_published') or 0

    print("=" * 50)
    print(f"🏁 Pipeline Run Complete — {datetime.utcnow().isoformat()}")
    print(f"   Cities fetched:      {cities}")
    print(f"   Messages published:  {published}")
    print("=" * 50)


# ── Tasks ──────────────────────────────────────────────────────

fetch_and_publish = PythonOperator(
    task_id='fetch_and_publish',
    python_callable=task_fetch_and_publish,
    dag=dag,
)

# Give the consumer ~30s to process the Kafka messages before S3 backup
wait_for_consumer = BashOperator(
    task_id='wait_for_consumer',
    bash_command='sleep 30',
    dag=dag,
)

upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=task_upload_transformed_to_s3,
    dag=dag,
)

log_stats = PythonOperator(
    task_id='log_stats',
    python_callable=task_log_pipeline_stats,
    dag=dag,
)

# ── Pipeline order ─────────────────────────────────────────────
# fetch_and_publish → wait_for_consumer → upload_to_s3 → log_stats
fetch_and_publish >> wait_for_consumer >> upload_to_s3 >> log_stats
