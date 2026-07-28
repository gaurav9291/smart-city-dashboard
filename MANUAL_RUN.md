# Manual Run Guide

Run the project without Airflow.

## 1. Go to the project folder

```bash
cd /home/gaurav/Downloads/PROJECT
```

## 2. Start Elasticsearch and Kibana

```bash
docker compose up -d
```

Check Elasticsearch:

```bash
curl http://localhost:9200
```

Check Kibana in the browser:

```text
http://localhost:5601
```

If Elasticsearch reports shard/disk allocation problems, run:

```bash
curl -X PUT 'http://localhost:9200/_cluster/settings' \
  -H 'Content-Type: application/json' \
  -d '{"transient":{"cluster.routing.allocation.disk.watermark.low":"95%","cluster.routing.allocation.disk.watermark.high":"97%","cluster.routing.allocation.disk.watermark.flood_stage":"98%"}}'
```

Install the index template:

```bash
curl -X PUT 'http://localhost:9200/_index_template/smart-city-unified-template' \
  -H 'Content-Type: application/json' \
  --data-binary @elasticsearch_index_template.json
```

## 3. Start Zookeeper

Open a new terminal:

```bash
/home/gaurav/kafka_2.12-2.7.0/bin/zookeeper-server-start.sh /home/gaurav/kafka_2.12-2.7.0/config/zookeeper.properties
```

## 4. Start Kafka

Open a new terminal:

```bash
/home/gaurav/kafka_2.12-2.7.0/bin/kafka-server-start.sh /home/gaurav/kafka_2.12-2.7.0/config/server.properties
```

If Kafka says `/brokers/ids/0` already exists, wait a few seconds and run the Kafka command again.

## 5. Start the producers

Open three separate terminals:

```bash
python3 aqi_producer.py
```

```bash
python3 traffic_producer.py
```

```bash
python3 weather_producer.py
```

## 6. Start the unified consumer

Open a new terminal:

```bash
/home/gaurav/Downloads/0setup/spark-4.1.1-bin-hadoop3/bin/spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  unified_consumer.py
```

Spark UI:

```text
http://localhost:4040
```

## 7. Start the dashboard API

Open a new terminal:

```bash
python3 -m uvicorn dashboard_api:app --host 0.0.0.0 --port 8000
```

Dashboard:

```text
http://localhost:8000
```

## Quick checks

```bash
curl http://localhost:8000/api/health
curl http://localhost:8000/api/summary
curl http://localhost:9200/smart-city-unified/_count
```

## Stop everything

Press `Ctrl+C` in the Zookeeper, Kafka, producer, Spark, and API terminals.

Then stop Docker services:

```bash
docker compose down
```

If you started Elasticsearch/Kibana manually instead of using Compose:

```bash
docker stop smartcity-kibana-manual smartcity-elasticsearch-manual
docker rm smartcity-kibana-manual smartcity-elasticsearch-manual
```
