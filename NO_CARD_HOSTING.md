# No-Card Hosting Guide

This is the best setup when signups are okay, but you do not have a debit or credit card:

```text
Render free web app -> Aiven free Kafka -> Databricks Free Edition -> Aiven free OpenSearch -> Render dashboard
```

The Render app runs the dashboard and all three producers in one service using `online_app.py`.

## 1. Create Aiven Kafka

1. Go to Aiven and create a free account.
2. Create a free **Apache Kafka** service.
3. Create these topics:
   - `aqi-data`
   - `traffic-data`
   - `weather-data`
4. Copy the Kafka connection values:
   - service URI / bootstrap server
   - username
   - password

Use them as:

```text
KAFKA_BOOTSTRAP_SERVERS=<aiven-kafka-host>:<aiven-kafka-port>
KAFKA_USERNAME=<aiven-kafka-username>
KAFKA_PASSWORD=<aiven-kafka-password>
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SSL_CA_PEM=<optional-aiven-ca-certificate>
```

If Aiven gives you a CA certificate, paste the whole certificate into `KAFKA_SSL_CA_PEM`. Keep the begin/end certificate lines. If your host does not like multiline values, replace line breaks with `\n`.

## 2. Create Aiven OpenSearch

1. In Aiven, create a free **OpenSearch** service.
2. Copy:
   - service URI
   - username
   - password

Use them as:

```text
ELASTICSEARCH_URL=<aiven-opensearch-service-uri>
ELASTICSEARCH_USERNAME=<aiven-opensearch-username>
ELASTICSEARCH_PASSWORD=<aiven-opensearch-password>
ELASTICSEARCH_INDEX=smart-city-unified
```

The project still uses environment variable names beginning with `ELASTICSEARCH_`, but they can point to OpenSearch.

## 3. Push Project to GitHub

From the project folder:

```bash
git init
git add .
git commit -m "Prepare no-card cloud hosting"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main
```

If your repo already exists, just run:

```bash
git add .
git commit -m "Prepare no-card cloud hosting"
git push
```

## 4. Deploy Render Web App

1. Go to Render.
2. Create a new **Blueprint**.
3. Connect your GitHub repo.
4. Render reads `render.yaml`.
5. It creates one service:
   - `smart-city-online`
6. Add the environment variables from Aiven.
7. Deploy.

The service start command is:

```bash
python online_app.py
```

This starts:

- FastAPI dashboard
- AQI producer
- traffic producer
- weather producer

## 5. Confirm Kafka Has Messages

In Aiven Kafka:

1. Open each topic.
2. Check messages/metrics.
3. Confirm data is arriving for:
   - `aqi-data`
   - `traffic-data`
   - `weather-data`

If messages are not arriving, check Render logs.

## 6. Set Up Databricks Free Edition

1. Sign up for Databricks Free Edition.
2. Upload or connect this GitHub repo.
3. Create a job or notebook that runs `unified_consumer.py`.
4. Make sure `cloud_config.py` is in the same folder.
5. Add the Spark Kafka connector matching your Spark version.
6. Add the same Kafka/OpenSearch environment variables to the Databricks compute/job.

Required environment variables:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_USERNAME
KAFKA_PASSWORD
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SSL_CA_PEM
KAFKA_TOPICS=aqi-data,traffic-data,weather-data
KAFKA_STARTING_OFFSETS=latest

ELASTICSEARCH_URL
ELASTICSEARCH_USERNAME
ELASTICSEARCH_PASSWORD
ELASTICSEARCH_INDEX=smart-city-unified
```

## 7. Start Databricks Consumer

Run `unified_consumer.py`.

Successful logs look like:

```text
Unified Multi-Topic Consumer Online
Indexed 5 records into smart-city-unified.
```

## 8. Open Dashboard

Open your Render URL:

```text
https://smart-city-online.onrender.com
```

Test:

```text
/api/health
/api/summary
/
```

## Important Free-Tier Caveat

Render free web services may sleep when inactive. When the service sleeps, the producers stop. Open the dashboard URL again to wake it up.

For a demo, this is usually fine. For a production always-on pipeline, you eventually need a paid worker or another always-on host.
