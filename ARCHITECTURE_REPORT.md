# 🏙️ Smart City Data Pipeline - Architecture Report

## 📊 Project Overview

- **Project Type:** Real-time data pipeline for smart city metrics
- **Main Components:** 907 lines of Python code
- **Architecture:** Event-driven, distributed streaming
- **Orchestration:** Apache Airflow

---

## 🔌 Data Flow Architecture

```
  ┌─────────────────────┐
  │   DATA PRODUCERS    │
  ├─────────────────────┤
  │ • AQI Producer      │ → Measures air quality
  │ • Traffic Producer  │ → Tracks vehicle patterns  
  │ • Weather Producer  │ → Environmental conditions
  └──────────┬──────────┘
             │
             ↓ (Kafka Topics)
     ┌───────────────────┐
     │   MESSAGE QUEUE   │
     │  (Apache Kafka)   │
     │ localhost:9092    │
     └───────────┬───────┘
             │
             ↓
  ┌──────────────────────────────────┐
  │   DATA PROCESSING               │
  ├──────────────────────────────────┤
  │  Spark Unified Consumer          │
  │  (Stream Processing & Analytics) │
  └──────────────────┬───────────────┘
                     │
                     ↓
           ┌─────────────────────┐
           │ ELASTICSEARCH 8.13  │
           │ (Search & Storage)  │
           └──────────┬──────────┘
                      │
        ┌─────────────┴──────────────┐
        ↓                            ↓
   ┌────────────┐          ┌──────────────────┐
   │  KIBANA    │          │  FastAPI         │
   │  Dashboard │          │  Dashboard API   │
   │  UI        │          │  (localhost:8000)│
   └────────────┘          └──────────────────┘
```

---

## 📁 Project Structure

```
PROJECT/
├── aqi_producer.py              Generates air quality index data
├── aqi_consumer.py              Consumes and processes AQI data
├── traffic_producer.py          Generates traffic sensor data
├── traffic_consumer.py          Processes traffic metrics
├── weather_producer.py          Generates weather observations
├── weather_consumer.py          Processes weather data
├── unified_consumer.py          Spark consumer for stream aggregation
├── dashboard_api.py             FastAPI backend (port 8000)
├── docker-compose.yml           Elasticsearch & Kibana setup
├── elasticsearch_index_template.json  Schema definition
├── AIRFLOW.md                   Orchestration documentation
├── airflow/dags/                Airflow DAG definitions
└── runtime/                     Process logs and PIDs
```

---

## 🔄 Pipeline Workflow

### 1. Docker Compose Stage
- Launches Elasticsearch (port 9200)
- Launches Kibana (port 5601)

### 2. Producer Stage (Real-time Data Ingestion)
- AQI Producer streams pollution data
- Traffic Producer streams congestion metrics
- Weather Producer streams environmental data

### 3. Message Queue Stage (Apache Kafka)
- Decouples producers from consumers
- Enables scalable, reliable message delivery

### 4. Consumer & Aggregation Stage (Spark)
- unified_consumer processes streams in real-time
- Aggregates data across multiple sources
- Transforms to analytics-ready format

### 5. Storage & Search Stage (Elasticsearch)
- Indexes enriched data for fast queries
- Maintains time-series data for historical analysis

### 6. Visualization & API Stage
- Kibana provides visual dashboards
- FastAPI serves custom metrics via REST API

---

## 🚀 Key Technologies

| Technology | Purpose |
|-----------|---------|
| Apache Kafka | Message streaming & event queuing |
| Apache Spark | Distributed stream processing |
| Elasticsearch | Full-text search & analytics |
| Apache Airflow | Workflow orchestration |
| FastAPI | Modern Python web API |
| Docker Compose | Container orchestration |
| Kibana | Data visualization |

---

## ⚙️ System Configuration

- **Kafka Broker:** localhost:9092
- **Elasticsearch:** localhost:9200
- **Kibana UI:** localhost:5601
- **Dashboard API:** localhost:8000
- **Elasticsearch Memory:** 1GB (configurable via ES_JAVA_OPTS)
- **Network Mode:** Host (for inter-container communication)

---

## 🛠️ Process Management

All producer, consumer, and API processes are managed by Airflow:
- **Log Location:** `runtime/logs/`
- **PID Location:** `runtime/pids/`
- **Status Check:** Check `runtime/pids/*.pid` files

---

## 📈 Deployment Checklist

### Install Python dependencies:
```bash
pip install airflow fastapi kafka-python pyspark requests uvicorn
```

### Setup Airflow DAG:
```bash
mkdir -p ~/airflow/dags
ln -sf /home/gaurav/Downloads/PROJECT/airflow/dags/smart_city_pipeline.py ~/airflow/dags/
```

### Start Airflow:
```bash
airflow standalone
```

### Prerequisites:
- Docker & Docker Compose
- Kafka running on localhost:9092
- Python 3.x
- Spark (for spark-submit)

---

## 🎯 Use Cases

- Urban air quality monitoring
- Real-time traffic congestion analysis
- Weather-based city event correlation
- Historical trend analysis via Kibana
- RESTful API for custom queries
- Automated alerts on metric thresholds
