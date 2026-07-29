# Extremely Detailed No-Card Hosting Guide

This guide is for hosting this project online when:

- You can create free accounts.
- You do **not** have a debit card or credit card.
- You want the easiest realistic cloud setup.
- You want to avoid getting lost between platforms.

Follow this guide in order. Do not skip checkpoints.

---

## 0. Final Architecture

You will host the project like this:

```text
Render free web app
  ├─ runs dashboard_api.py
  ├─ runs aqi_producer.py in background
  ├─ runs traffic_producer.py in background
  └─ runs weather_producer.py in background

        ↓ sends messages to

Aiven Free Kafka
  ├─ topic: aqi-data
  ├─ topic: traffic-data
  └─ topic: weather-data

        ↓ read by

Databricks Free Edition
  └─ runs unified_consumer.py

        ↓ writes documents to

Aiven Free OpenSearch
  └─ index: smart-city-unified

        ↓ queried by

Render public dashboard URL
```

The important idea:

```text
Render produces and displays.
Aiven Kafka stores stream messages.
Databricks processes stream messages.
Aiven OpenSearch stores processed dashboard records.
```

---

## 1. Files That Matter

These are the most important files in this repo:

| File | Purpose |
|---|---|
| `online_app.py` | Starts the dashboard and all 3 producers together |
| `render.yaml` | Tells Render how to deploy the free web app |
| `cloud_config.py` | Reads Kafka/OpenSearch credentials from environment variables |
| `aqi_producer.py` | Sends AQI data to Kafka |
| `traffic_producer.py` | Sends traffic data to Kafka |
| `weather_producer.py` | Sends weather data to Kafka |
| `unified_consumer.py` | Databricks Spark job that reads Kafka and writes OpenSearch |
| `dashboard_api.py` | FastAPI backend and static dashboard host |
| `.env.example` | List of environment variables you need |
| `requirements.txt` | Python dependencies |

Render will run this:

```bash
python online_app.py
```

Databricks will run this:

```bash
unified_consumer.py
```

---

## 2. Accounts You Need

You need these free accounts:

| Platform | Why You Need It | Card Needed? |
|---|---|---|
| GitHub | Store your project code | No |
| Aiven | Free Kafka and free OpenSearch | No card for free tier |
| Render | Public dashboard and producers | No card for free web service |
| Databricks Free Edition | Run Spark streaming consumer | No card |

Use the **Free Edition / Free Tier** paths, not paid trials that ask for billing.

---

## 3. Values You Will Collect

Keep a temporary notes file while doing setup. You need these values:

```text
KAFKA_BOOTSTRAP_SERVERS=
KAFKA_USERNAME=
KAFKA_PASSWORD=
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SSL_CA_PEM=

ELASTICSEARCH_URL=
ELASTICSEARCH_USERNAME=
ELASTICSEARCH_PASSWORD=
ELASTICSEARCH_INDEX=smart-city-unified

WAQI_TOKEN=
```

Important:

- `ELASTICSEARCH_*` variables will point to **Aiven OpenSearch**.
- The code keeps the name `ELASTICSEARCH_*` because OpenSearch supports similar APIs.
- `KAFKA_SSL_CA_PEM` may be empty if Aiven does not require you to paste a CA certificate.

---

## 4. Phase 1 — Prepare GitHub

### 4.1 Open Terminal

Go to your project:

```bash
cd /home/gaurav/Downloads/PROJECT
```

### 4.2 Confirm Important Files Exist

Run:

```bash
ls
```

You should see at least:

```text
online_app.py
render.yaml
cloud_config.py
NO_CARD_HOSTING.md
requirements.txt
aqi_producer.py
traffic_producer.py
weather_producer.py
unified_consumer.py
dashboard_api.py
dashboard
```

If `online_app.py` is missing, stop. The no-card Render setup depends on it.

### 4.3 Initialize Git

Run:

```bash
git init
```

If it says it already exists, that is okay.

### 4.4 Add Files

Run:

```bash
git add .
```

### 4.5 Commit Files

Run:

```bash
git commit -m "Prepare no-card online hosting"
```

If Git asks for username/email, run:

```bash
git config --global user.name "Gaurav"
git config --global user.email "your-email@example.com"
```

Then run the commit again:

```bash
git commit -m "Prepare no-card online hosting"
```

### 4.6 Create GitHub Repository

Open:

```text
https://github.com/new
```

Create a repository named:

```text
smart-city-online
```

Keep it public or private. Public is easier for free deployments.

Do **not** add README, `.gitignore`, or license from GitHub if your local project already has files.

### 4.7 Push Code to GitHub

Replace `<your-username>` with your GitHub username:

```bash
git branch -M main
git remote add origin https://github.com/<your-username>/smart-city-online.git
git push -u origin main
```

If `remote origin already exists`, run:

```bash
git remote -v
```

If the URL is wrong, fix it:

```bash
git remote set-url origin https://github.com/<your-username>/smart-city-online.git
git push -u origin main
```

### 4.8 Checkpoint

Before continuing:

- Open your GitHub repo in the browser.
- Confirm `online_app.py` is visible.
- Confirm `render.yaml` is visible.
- Confirm `unified_consumer.py` is visible.

Do not continue until this is true.

---

## 5. Phase 2 — Create Aiven Free Kafka

### 5.1 Create Aiven Account

Open:

```text
https://aiven.io/free-tier
```

Sign up for a free account.

Choose the no-card free tier path.

### 5.2 Create a Project

After logging in:

1. Go to the Aiven console.
2. Create a project if Aiven asks.
3. Name it something simple:

```text
smart-city-project
```

### 5.3 Create Kafka Service

Create a new service:

```text
Apache Kafka
```

Choose:

```text
Free tier
```

Name it:

```text
smart-city-kafka
```

Pick a region close to you if available.

Wait until service status is:

```text
Running
```

### 5.4 Create Kafka Topics

Inside the Kafka service, create exactly these topics:

```text
aqi-data
traffic-data
weather-data
```

Use default settings.

### 5.5 Find Kafka Connection Details

In the Aiven Kafka service, look for:

```text
Connection information
```

You need:

```text
host
port
username
password
```

Build:

```text
KAFKA_BOOTSTRAP_SERVERS=<host>:<port>
KAFKA_USERNAME=<username>
KAFKA_PASSWORD=<password>
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
```

Example shape:

```text
KAFKA_BOOTSTRAP_SERVERS=smart-city-kafka-yourproject.a.aivencloud.com:12345
KAFKA_USERNAME=avnadmin
KAFKA_PASSWORD=some-long-password
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
```

### 5.6 Handle Aiven CA Certificate

Aiven may show a CA certificate.

If Aiven gives you a certificate, copy the whole thing:

```text
-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----
```

Save it as:

```text
KAFKA_SSL_CA_PEM
```

If Render accepts multiline env vars, paste it normally.

If Render does not accept multiline text, replace each newline with `\n`.

Example:

```text
-----BEGIN CERTIFICATE-----\nabc123\n-----END CERTIFICATE-----
```

If Aiven Kafka works without pasting the CA, you can leave:

```text
KAFKA_SSL_CA_PEM=
```

### 5.7 Checkpoint

You should now have:

```text
KAFKA_BOOTSTRAP_SERVERS=
KAFKA_USERNAME=
KAFKA_PASSWORD=
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SSL_CA_PEM=
```

And these topics:

```text
aqi-data
traffic-data
weather-data
```

Do not continue until Kafka is running.

---

## 6. Phase 3 — Create Aiven Free OpenSearch

### 6.1 Create OpenSearch Service

In Aiven, create another service:

```text
OpenSearch
```

Choose:

```text
Free tier
```

Name it:

```text
smart-city-opensearch
```

Wait until service status is:

```text
Running
```

### 6.2 Get OpenSearch Connection Details

Open the OpenSearch service connection information.

You need:

```text
Service URI
Username
Password
```

Use them as:

```text
ELASTICSEARCH_URL=<service-uri>
ELASTICSEARCH_USERNAME=<username>
ELASTICSEARCH_PASSWORD=<password>
ELASTICSEARCH_INDEX=smart-city-unified
```

Example shape:

```text
ELASTICSEARCH_URL=https://smart-city-opensearch-yourproject.a.aivencloud.com:12345
ELASTICSEARCH_USERNAME=avnadmin
ELASTICSEARCH_PASSWORD=some-long-password
ELASTICSEARCH_INDEX=smart-city-unified
```

### 6.3 Checkpoint

You should now have:

```text
ELASTICSEARCH_URL=
ELASTICSEARCH_USERNAME=
ELASTICSEARCH_PASSWORD=
ELASTICSEARCH_INDEX=smart-city-unified
```

Do not continue until OpenSearch is running.

---

## 7. Phase 4 — Deploy Render Free Web App

Render will run:

- Dashboard API
- Dashboard UI
- AQI producer
- Traffic producer
- Weather producer

All inside one free web service.

### 7.1 Create Render Account

Open:

```text
https://render.com
```

Sign up using GitHub if possible.

No card should be needed for a free web service.

### 7.2 Create Blueprint

In Render:

1. Click **New**.
2. Choose **Blueprint**.
3. Connect your GitHub account if needed.
4. Select your repo:

```text
smart-city-online
```

Render should detect:

```text
render.yaml
```

### 7.3 Confirm Render Service

Render should create one service:

```text
smart-city-online
```

It should use:

```text
runtime: python
plan: free
buildCommand: pip install -r requirements.txt
startCommand: python online_app.py
```

These values are already in `render.yaml`.

### 7.4 Add Environment Variables in Render

Render will ask for secret environment variables.

Add Kafka values:

```text
KAFKA_BOOTSTRAP_SERVERS=<your-aiven-kafka-host:port>
KAFKA_USERNAME=<your-aiven-kafka-username>
KAFKA_PASSWORD=<your-aiven-kafka-password>
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SSL_CA_PEM=<optional-aiven-ca-certificate>
```

Add OpenSearch values:

```text
ELASTICSEARCH_URL=<your-aiven-opensearch-uri>
ELASTICSEARCH_USERNAME=<your-aiven-opensearch-username>
ELASTICSEARCH_PASSWORD=<your-aiven-opensearch-password>
ELASTICSEARCH_INDEX=smart-city-unified
```

Add WAQI token:

```text
WAQI_TOKEN=demo
```

If you have a real WAQI token, use it instead:

```text
WAQI_TOKEN=<your-real-waqi-token>
```

`demo` is okay because the AQI producer has fallback mock data if the WAQI API does not return live data. Traffic is fully simulated, and weather uses Open-Meteo without an API token.

### 7.5 Deploy

Click:

```text
Apply
```

or:

```text
Deploy
```

Wait for build to finish.

### 7.6 Check Render Logs

Open:

```text
smart-city-online → Logs
```

You want to see:

```text
Kafka Producer initialized successfully
Traffic Kafka Producer initialized successfully
Weather Kafka Producer initialized
```

You should also see producer messages every 15 seconds.

### 7.7 If Kafka Fails on Render

If logs show:

```text
Failed to initialize Kafka Producer
```

Check these in Render env vars:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_USERNAME
KAFKA_PASSWORD
KAFKA_SECURITY_PROTOCOL
KAFKA_SASL_MECHANISM
KAFKA_SSL_CA_PEM
```

Most common mistakes:

- Missing port in `KAFKA_BOOTSTRAP_SERVERS`.
- Wrong username/password.
- Forgot `SASL_SSL`.
- Pasted certificate incorrectly.

### 7.8 Checkpoint

Before continuing:

- Render app is deployed.
- Render logs show producers running.
- No repeated Kafka connection errors.

Do not continue until this works.

---

## 8. Phase 5 — Confirm Kafka Receives Data

Open Aiven Kafka service.

Check topic metrics/messages for:

```text
aqi-data
traffic-data
weather-data
```

You should see messages coming in.

If Aiven has a message browser, open each topic.

If Aiven only shows metrics, check produce rate or bytes in.

### 8.1 Checkpoint

Do not continue until:

```text
aqi-data has messages
traffic-data has messages
weather-data has messages
```

If Kafka has no messages, Databricks will have nothing to process.

---

## 9. Phase 6 — Set Up Databricks Free Edition

Databricks will run the Spark streaming file:

```text
unified_consumer.py
```

### 9.1 Create Databricks Free Edition Account

Open:

```text
https://login.databricks.com/signup
```

Choose:

```text
Free Edition
```

Avoid paid cloud trial paths if they ask for billing.

### 9.2 Open Workspace

After signup, open your Databricks workspace.

### 9.3 Add Your GitHub Repo

In Databricks:

1. Open **Workspace**.
2. Find **Repos** or **Git folders**.
3. Add your GitHub repo:

```text
https://github.com/<your-username>/smart-city-online.git
```

Confirm these files are visible in Databricks:

```text
unified_consumer.py
cloud_config.py
requirements.txt
```

Very important:

```text
cloud_config.py must be in the same folder as unified_consumer.py
```

### 9.4 Decide Job or Notebook

Use a **Job** if Databricks Free Edition allows it in your workspace.

If Jobs are confusing or unavailable, use a notebook that imports/runs the script.

Job is cleaner. Notebook is easier to debug.

---

## 10. Phase 7 — Add Databricks Environment Variables

Databricks must know the same Kafka/OpenSearch values.

Use these values:

```text
KAFKA_BOOTSTRAP_SERVERS=<your-aiven-kafka-host:port>
KAFKA_USERNAME=<your-aiven-kafka-username>
KAFKA_PASSWORD=<your-aiven-kafka-password>
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SSL_CA_PEM=<optional-aiven-ca-certificate>
KAFKA_TOPICS=aqi-data,traffic-data,weather-data
KAFKA_STARTING_OFFSETS=latest

ELASTICSEARCH_URL=<your-aiven-opensearch-uri>
ELASTICSEARCH_USERNAME=<your-aiven-opensearch-username>
ELASTICSEARCH_PASSWORD=<your-aiven-opensearch-password>
ELASTICSEARCH_INDEX=smart-city-unified
```

### 10.1 If Using Databricks Job Cluster Spark Config

Paste this into cluster Spark config.

Replace values manually if secret syntax is not available in Free Edition:

```text
spark.driverEnv.KAFKA_BOOTSTRAP_SERVERS <your-aiven-kafka-host:port>
spark.driverEnv.KAFKA_USERNAME <your-aiven-kafka-username>
spark.driverEnv.KAFKA_PASSWORD <your-aiven-kafka-password>
spark.driverEnv.KAFKA_SECURITY_PROTOCOL SASL_SSL
spark.driverEnv.KAFKA_SASL_MECHANISM PLAIN
spark.driverEnv.KAFKA_SSL_CA_PEM <optional-aiven-ca-certificate>
spark.driverEnv.KAFKA_TOPICS aqi-data,traffic-data,weather-data
spark.driverEnv.KAFKA_STARTING_OFFSETS latest
spark.driverEnv.ELASTICSEARCH_URL <your-aiven-opensearch-uri>
spark.driverEnv.ELASTICSEARCH_USERNAME <your-aiven-opensearch-username>
spark.driverEnv.ELASTICSEARCH_PASSWORD <your-aiven-opensearch-password>
spark.driverEnv.ELASTICSEARCH_INDEX smart-city-unified

spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS <your-aiven-kafka-host:port>
spark.executorEnv.KAFKA_USERNAME <your-aiven-kafka-username>
spark.executorEnv.KAFKA_PASSWORD <your-aiven-kafka-password>
spark.executorEnv.KAFKA_SECURITY_PROTOCOL SASL_SSL
spark.executorEnv.KAFKA_SASL_MECHANISM PLAIN
spark.executorEnv.KAFKA_SSL_CA_PEM <optional-aiven-ca-certificate>
spark.executorEnv.KAFKA_TOPICS aqi-data,traffic-data,weather-data
spark.executorEnv.KAFKA_STARTING_OFFSETS latest
spark.executorEnv.ELASTICSEARCH_URL <your-aiven-opensearch-uri>
spark.executorEnv.ELASTICSEARCH_USERNAME <your-aiven-opensearch-username>
spark.executorEnv.ELASTICSEARCH_PASSWORD <your-aiven-opensearch-password>
spark.executorEnv.ELASTICSEARCH_INDEX smart-city-unified
```

If `KAFKA_SSL_CA_PEM` is empty, you can omit both `KAFKA_SSL_CA_PEM` lines.

### 10.2 If Using a Databricks Notebook

At the top of the notebook, before running the consumer, set:

```python
import os

os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "<your-aiven-kafka-host:port>"
os.environ["KAFKA_USERNAME"] = "<your-aiven-kafka-username>"
os.environ["KAFKA_PASSWORD"] = "<your-aiven-kafka-password>"
os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL"
os.environ["KAFKA_SASL_MECHANISM"] = "PLAIN"
os.environ["KAFKA_TOPICS"] = "aqi-data,traffic-data,weather-data"
os.environ["KAFKA_STARTING_OFFSETS"] = "latest"

os.environ["ELASTICSEARCH_URL"] = "<your-aiven-opensearch-uri>"
os.environ["ELASTICSEARCH_USERNAME"] = "<your-aiven-opensearch-username>"
os.environ["ELASTICSEARCH_PASSWORD"] = "<your-aiven-opensearch-password>"
os.environ["ELASTICSEARCH_INDEX"] = "smart-city-unified"
```

If you have a CA certificate:

```python
os.environ["KAFKA_SSL_CA_PEM"] = """-----BEGIN CERTIFICATE-----
paste certificate here
-----END CERTIFICATE-----"""
```

---

## 11. Phase 8 — Add Spark Kafka Connector in Databricks

Spark cannot read Kafka unless the Kafka connector is available.

### 11.1 Find Spark Version

In Databricks, check the runtime or Spark version.

It may look like:

```text
Spark 3.5.0
Scala 2.12
```

### 11.2 Add Maven Package

Add a Maven library:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:<spark-version>
```

Examples:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2
```

Use the version matching your Spark version.

### 11.3 Add Python Library

Add PyPI library:

```text
requests
```

You do not need to install `pyspark`; Databricks already provides Spark.

---

## 12. Phase 9 — Run Databricks Consumer

### 12.1 If Running as a Job

Create a Databricks Job:

```text
Task type: Python script
Script path: unified_consumer.py
```

Start the job.

### 12.2 If Running from Notebook

Create a notebook in the same folder as the project files.

Set environment variables first.

Then run:

```python
exec(open("unified_consumer.py").read())
```

If Databricks cannot find the file, use the full workspace path or open the repo folder notebook.

### 12.3 Expected Logs

Successful startup:

```text
Unified Multi-Topic Consumer Online
```

Successful processing:

```text
Unified Batch: 1
Indexed 5 records into smart-city-unified.
```

Early startup may show:

```text
No Kafka records received in this trigger.
```

That is okay for a short time. If it continues for several minutes, Kafka messages are not reaching Databricks.

### 12.4 Checkpoint

Before continuing:

- Databricks consumer is running.
- Logs show `Unified Multi-Topic Consumer Online`.
- Logs eventually show `Indexed`.

---

## 13. Phase 10 — Confirm OpenSearch Has Data

Open Aiven OpenSearch.

Look for a console, dashboard, or API test area.

You need to confirm index:

```text
smart-city-unified
```

exists and has documents.

If using curl locally:

```bash
curl -u "<ELASTICSEARCH_USERNAME>:<ELASTICSEARCH_PASSWORD>" \
  "<ELASTICSEARCH_URL>/smart-city-unified/_count"
```

Expected result:

```json
{
  "count": 5
}
```

The count should increase over time.

### 13.1 Checkpoint

Before opening the dashboard:

- OpenSearch index exists.
- Count is greater than zero.

---

## 14. Phase 11 — Open Public Dashboard

Open Render.

Open the service:

```text
smart-city-online
```

Copy the public URL.

It will look like:

```text
https://smart-city-online.onrender.com
```

Test health:

```text
https://smart-city-online.onrender.com/api/health
```

Expected:

```json
{
  "status": "ok",
  "elasticsearch": "...",
  "index": "smart-city-unified"
}
```

Test summary:

```text
https://smart-city-online.onrender.com/api/summary
```

Expected:

```json
{
  "zone_count": 5,
  "record_count": ...
}
```

Open dashboard:

```text
https://smart-city-online.onrender.com/
```

Before Databricks writes the first records, the dashboard may show empty cards or zero values. That is normal. Data appears only after `unified_consumer.py` creates documents in `smart-city-unified`.

---

## 15. Correct Startup Order

If you ever restart everything, use this order:

```text
1. Aiven Kafka running
2. Aiven OpenSearch running
3. Render smart-city-online running
4. Confirm Kafka topics receive messages
5. Databricks unified_consumer.py running
6. Confirm OpenSearch count increases
7. Open Render dashboard URL
```

Do not start Databricks first if producers are asleep.

---

## 16. Free-Tier Caveats

### Render Sleep

Render free web services can sleep when inactive.

When Render sleeps:

- Dashboard sleeps.
- Producers stop.
- Kafka messages stop.

To wake it:

```text
Open the Render dashboard URL
```

Wait 30–90 seconds.

### Databricks Free Edition Limits

Databricks Free Edition has limits.

If a job stops because of usage or idle limits, restart it manually.

### Aiven Free Tier Limits

Aiven free services have limits on throughput, topics, retention, and storage.

Your demo stream is small, so it should fit.

---

## 17. Troubleshooting

### Problem A — Render Build Fails

Check Render logs.

Common causes:

- GitHub repo missing `requirements.txt`.
- Python dependency install failed.
- Wrong start command.

Expected start command:

```bash
python online_app.py
```

### Problem B — Render Starts but Kafka Producer Fails

Look for:

```text
Failed to initialize Kafka Producer
```

Check:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_USERNAME
KAFKA_PASSWORD
KAFKA_SECURITY_PROTOCOL
KAFKA_SASL_MECHANISM
KAFKA_SSL_CA_PEM
```

Try setting `KAFKA_SSL_CA_PEM` if Aiven provides a CA certificate.

### Problem C — Kafka Topics Are Empty

This means Render is not producing.

Check:

```text
Render logs
Kafka credentials
Kafka topic names
```

Topic names must be exactly:

```text
aqi-data
traffic-data
weather-data
```

### Problem D — Databricks Cannot Find Kafka Source

Error may mention:

```text
Failed to find data source: kafka
```

Fix:

Add Spark Kafka Maven package:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:<spark-version>
```

### Problem E — Databricks Reads No Data

If logs repeatedly show:

```text
No Kafka records received in this trigger.
```

Check:

1. Render app is awake.
2. Kafka topics have messages.
3. Databricks Kafka env vars are correct.
4. `KAFKA_TOPICS=aqi-data,traffic-data,weather-data`.

### Problem F — Databricks Cannot Write to OpenSearch

Check:

```text
ELASTICSEARCH_URL
ELASTICSEARCH_USERNAME
ELASTICSEARCH_PASSWORD
```

Remember: these point to Aiven OpenSearch.

### Problem G — Dashboard `/api/health` Returns 502

Render dashboard cannot reach OpenSearch.

Check Render env vars:

```text
ELASTICSEARCH_URL
ELASTICSEARCH_USERNAME
ELASTICSEARCH_PASSWORD
```

### Problem H — Dashboard Opens but Shows No Data

Check in this exact order:

```text
1. Render logs show producers sending data
2. Aiven Kafka topics have messages
3. Databricks logs show Indexed records
4. OpenSearch count is greater than zero
5. Render dashboard env vars point to same OpenSearch service
```

---

## 18. Final Checklist

Only tick a box when it is actually working.

```text
[ ] GitHub repo created
[ ] Code pushed to GitHub
[ ] Aiven Kafka service running
[ ] Kafka topics created: aqi-data, traffic-data, weather-data
[ ] Kafka username/password copied
[ ] Kafka CA certificate copied if needed
[ ] Aiven OpenSearch service running
[ ] OpenSearch URL/username/password copied
[ ] Render Blueprint connected to GitHub
[ ] Render service smart-city-online deployed
[ ] Render logs show all three producers initialized
[ ] Aiven Kafka topics show incoming messages
[ ] Databricks Free Edition account created
[ ] GitHub repo visible in Databricks
[ ] Databricks can see unified_consumer.py and cloud_config.py
[ ] Spark Kafka Maven connector added
[ ] requests Python library added
[ ] Databricks env vars configured
[ ] Databricks consumer starts
[ ] Databricks logs show Indexed records
[ ] OpenSearch smart-city-unified count is greater than zero
[ ] Render /api/health works
[ ] Render /api/summary works
[ ] Public dashboard opens
```

---

## 19. The Most Important Rule

When confused, debug from left to right:

```text
Render producers
  → Aiven Kafka
  → Databricks consumer
  → Aiven OpenSearch
  → Render dashboard
```

Do not debug the dashboard first if Kafka has no messages.

Do not debug Databricks first if Render producers are asleep.

Keep the pipeline order in your head and you will not get lost.
