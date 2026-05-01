# Real-Time Weather Data Pipeline

An end-to-end ETL pipeline that ingests real-time weather data from the OpenWeatherMap API, streams it through Apache Kafka, transforms it with Pandas, and loads it into PostgreSQL — with raw data backed up to AWS S3. Orchestrated via Apache Airflow and fully containerized with Docker.

## Architecture

```
OpenWeatherMap API
       │
       ▼
  [ingestion.py]          ← Extract: HTTP GET, 3 cities, every 5 min
       │
       ▼
 Kafka Producer           ← Publishes raw JSON to 'weather-raw' topic
       │
       ▼
 Kafka Consumer           ← Subscribes, triggers transform + load
       │
       ├──► [transformation.py]   ← Clean, validate, add derived fields
       │           │
       │           ▼
       │      [storage.py]        ← Load into PostgreSQL
       │
       └──► [s3_storage.py]       ← Upload raw JSON to AWS S3
```

Airflow DAG (`weather_etl_pipeline`) runs every 5 minutes and orchestrates the extract + publish steps, with a downstream task that backs up transformed CSVs to S3.

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, Requests, OpenWeatherMap API |
| Streaming | Apache Kafka + Zookeeper |
| Transformation | Python, Pandas |
| Storage | PostgreSQL, psycopg2 |
| Cloud backup | AWS S3, boto3 |
| Orchestration | Apache Airflow |
| Containerization | Docker, Docker Compose |

## Setup

### 1. Clone and configure

```bash
git clone <repo>
cd weather-data-pipeline
cp .env.example .env
# Fill in your API keys in .env
```

### 2. Start all services

```bash
docker-compose up -d
```

This starts: PostgreSQL, Zookeeper, Kafka, Airflow, and the pipeline app.

### 3. Access Airflow UI

Go to `http://localhost:8080` — login: `admin / admin`

Enable the `weather_etl_pipeline` DAG.

### 4. Run standalone (no Airflow)

```bash
python src/pipeline.py
```

### 5. Run consumer (separate terminal)

```bash
python src/kafka_consumer.py
```

## Project Structure

```
weather-data-pipeline/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── dags/
│   └── weather_pipeline_dag.py   # Airflow DAG
└── src/
    ├── config.py                 # Environment config
    ├── ingestion.py              # Extract — OpenWeatherMap API
    ├── transformation.py         # Transform — Pandas cleaning + validation
    ├── storage.py                # Load — PostgreSQL
    ├── s3_storage.py             # Load — AWS S3
    ├── kafka_producer.py         # Publish raw data to Kafka
    ├── kafka_consumer.py         # Consume, transform, load
    ├── pipeline.py               # Standalone end-to-end runner
    └── dashboard.py              # Visualization
```

## What it does

- Fetches current weather for Toronto, Montreal, and Vancouver every 5 minutes
- Validates data quality (temperature ranges, humidity bounds, null checks)
- Adds derived fields: temperature category, weather severity, time of day
- Stores structured records in PostgreSQL with full historical query support
- Backs up raw API responses and transformed CSVs to AWS S3
- Streams raw data through Kafka for decoupled, fault-tolerant ingestion
- Airflow DAG handles scheduling, retries, and observability
