# Airflow Integration

This project includes an Airflow DAG at:

```text
airflow/dags/smart_city_pipeline.py
```

The DAG automates the local Smart City pipeline:

1. Starts Elasticsearch and Kibana with Docker Compose.
2. Waits for Elasticsearch.
3. Installs the Elasticsearch index template.
4. Checks that Kafka is reachable at `localhost:9092`.
5. Starts the AQI, traffic, and weather producers.
6. Starts the Spark unified consumer.
7. Starts the FastAPI dashboard API.
8. Verifies `/api/health`.

## Install the DAG into Airflow

`airflow standalone` usually reads DAGs from `~/airflow/dags`.

Create the DAG folder if needed:

```bash
mkdir -p ~/airflow/dags
```

Symlink this project's DAG:

```bash
ln -sf /home/siddhesh/Documents/PROJECT/airflow/dags/smart_city_pipeline.py ~/airflow/dags/smart_city_pipeline.py
```

Then start Airflow:

```bash
airflow standalone
```

Open the Airflow UI, enable the `smart_city_pipeline` DAG, and trigger it manually.

## Prerequisites

Kafka must already be running on:

```text
localhost:9092
```

The DAG expects these commands to be available in the same environment where Airflow runs:

```text
docker
python3
spark-submit
uvicorn
```

Python dependencies used by the project include:

```text
airflow
fastapi
kafka-python
pyspark
requests
uvicorn
```

## Runtime Files

The DAG starts long-running scripts as detached local processes.

Logs are written to:

```text
runtime/logs/
```

PID files are written to:

```text
runtime/pids/
```

Example:

```bash
tail -f runtime/logs/unified_consumer.log
```

## Stop Running Pipeline Processes

To stop a process started by the DAG, use its PID file:

```bash
kill "$(cat runtime/pids/aqi_producer.pid)"
kill "$(cat runtime/pids/traffic_producer.pid)"
kill "$(cat runtime/pids/weather_producer.pid)"
kill "$(cat runtime/pids/unified_consumer.pid)"
kill "$(cat runtime/pids/dashboard_api.pid)"
```

To stop Elasticsearch and Kibana:

```bash
docker compose down
```

## Optional Environment Variables

You can override defaults before starting Airflow:

```bash
export SMART_CITY_PROJECT_DIR=/home/siddhesh/Documents/PROJECT
export ELASTICSEARCH_URL=http://localhost:9200
export SMART_CITY_API_URL=http://localhost:8000
export KAFKA_HOST=localhost
export KAFKA_PORT=9092
export SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1
```
