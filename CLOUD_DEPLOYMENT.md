# Cloud Deployment Guide

This is the easiest fully-online setup for this project:

```text
Render workers -> Confluent Cloud Kafka -> Databricks Job -> Elastic Cloud -> Render dashboard
```

Databricks runs the Spark streaming consumer. Render hosts the always-on producers and public dashboard. Confluent Cloud and Elastic Cloud replace local Kafka/Zookeeper and local Docker Elasticsearch.

## 1. Create Confluent Cloud Kafka

1. Create a Confluent Cloud cluster.
2. Create these topics:
   - `aqi-data`
   - `traffic-data`
   - `weather-data`
3. Create an API key and secret.
4. Save:
   - `KAFKA_BOOTSTRAP_SERVERS`
   - `KAFKA_USERNAME`
   - `KAFKA_PASSWORD`

## 2. Create Elastic Cloud

1. Create an Elastic Cloud deployment.
2. Copy the Elasticsearch endpoint.
3. Save:
   - `ELASTICSEARCH_URL`
   - `ELASTICSEARCH_USERNAME`
   - `ELASTICSEARCH_PASSWORD`

## 3. Push This Repo to GitHub

```bash
git init
git add .
git commit -m "Prepare cloud deployment"
git remote add origin <your-github-repo-url>
git push -u origin main
```

## 4. Deploy Producers and Dashboard on Render

1. In Render, create a new Blueprint.
2. Select this GitHub repo.
3. Render reads `render.yaml` and creates:
   - `smart-city-dashboard`
   - `smart-city-aqi-producer`
   - `smart-city-traffic-producer`
   - `smart-city-weather-producer`
4. Fill the secret environment variables from `.env.example`.
5. Deploy all services.

The dashboard will be public at the Render web service URL.

## 5. Create Databricks Secrets

```bash
databricks secrets create-scope smart-city
databricks secrets put-secret smart-city KAFKA_BOOTSTRAP_SERVERS
databricks secrets put-secret smart-city KAFKA_USERNAME
databricks secrets put-secret smart-city KAFKA_PASSWORD
databricks secrets put-secret smart-city ELASTICSEARCH_URL
databricks secrets put-secret smart-city ELASTICSEARCH_USERNAME
databricks secrets put-secret smart-city ELASTICSEARCH_PASSWORD
```

## 6. Create the Databricks Streaming Job

1. Open Databricks.
2. Go to **Workflows**.
3. Create a new job.
4. Add a **Python script** task.
5. Use `unified_consumer.py` from this GitHub repo or a Databricks workspace folder.
6. Use a standard job cluster.
7. Add PyPI library:
   - `requests`
8. Add the Spark Kafka Maven package that matches your cluster Spark version.

For many Databricks Runtime versions this looks like:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:<spark-version>
```

Check your cluster's Spark version before choosing the exact package version.

## 7. Add Databricks Job Environment Variables

In the job cluster Spark config, add:

```text
spark.driverEnv.KAFKA_BOOTSTRAP_SERVERS {{secrets/smart-city/KAFKA_BOOTSTRAP_SERVERS}}
spark.driverEnv.KAFKA_USERNAME {{secrets/smart-city/KAFKA_USERNAME}}
spark.driverEnv.KAFKA_PASSWORD {{secrets/smart-city/KAFKA_PASSWORD}}
spark.driverEnv.KAFKA_SECURITY_PROTOCOL SASL_SSL
spark.driverEnv.KAFKA_SASL_MECHANISM PLAIN
spark.driverEnv.ELASTICSEARCH_URL {{secrets/smart-city/ELASTICSEARCH_URL}}
spark.driverEnv.ELASTICSEARCH_USERNAME {{secrets/smart-city/ELASTICSEARCH_USERNAME}}
spark.driverEnv.ELASTICSEARCH_PASSWORD {{secrets/smart-city/ELASTICSEARCH_PASSWORD}}
spark.driverEnv.ELASTICSEARCH_INDEX smart-city-unified

spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS {{secrets/smart-city/KAFKA_BOOTSTRAP_SERVERS}}
spark.executorEnv.KAFKA_USERNAME {{secrets/smart-city/KAFKA_USERNAME}}
spark.executorEnv.KAFKA_PASSWORD {{secrets/smart-city/KAFKA_PASSWORD}}
spark.executorEnv.KAFKA_SECURITY_PROTOCOL SASL_SSL
spark.executorEnv.KAFKA_SASL_MECHANISM PLAIN
spark.executorEnv.ELASTICSEARCH_URL {{secrets/smart-city/ELASTICSEARCH_URL}}
spark.executorEnv.ELASTICSEARCH_USERNAME {{secrets/smart-city/ELASTICSEARCH_USERNAME}}
spark.executorEnv.ELASTICSEARCH_PASSWORD {{secrets/smart-city/ELASTICSEARCH_PASSWORD}}
spark.executorEnv.ELASTICSEARCH_INDEX smart-city-unified
```

## 8. Start and Verify

1. Start the three Render producer workers.
2. Start the Databricks job.
3. Wait until Databricks logs show:

```text
Unified Multi-Topic Consumer Online
Indexed records into smart-city-unified
```

4. Open the Render dashboard URL.
5. Check:

```text
/api/health
/api/summary
/
```

## Optional: Databricks Apps Dashboard

If you prefer hosting the dashboard inside Databricks instead of Render, use `app.yaml`:

```bash
databricks sync . /Workspace/Users/<your-email>/smart-city-project
databricks apps create smart-city-dashboard
databricks apps deploy smart-city-dashboard --source-code-path /Workspace/Users/<your-email>/smart-city-project
```

For a public URL, Render is usually simpler.
