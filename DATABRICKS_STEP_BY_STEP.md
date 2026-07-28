# Databricks Step-by-Step Guide

This guide covers **only** the Databricks part.

Your Render and Aiven setup is already separate. Databricks has one job:

```text
Read data from Aiven Kafka
Process it with Spark
Write final dashboard records to Aiven OpenSearch
```

Databricks runs this file:

```text
unified_consumer.py
```

---

## 0. Do Not Overthink the Options

When Databricks gives you many choices, use these:

```text
Account type: Databricks Free Edition
Code source: GitHub repo if available, otherwise upload files
Run method: Notebook fallback is easiest
Compute: whatever Free Edition gives you by default
Libraries needed: requests + Spark Kafka connector
Script to run: unified_consumer.py
```

Recommended beginner path:

```text
Use a Databricks notebook to run unified_consumer.py
```

Why notebook?

- Easier to paste environment variables.
- Easier to see errors.
- Less confusing than Jobs/Workflows.
- Good enough for demo hosting.

---

## 1. What You Need Before Starting

You need these values from Aiven.

### Kafka Values

```text
KAFKA_BOOTSTRAP_SERVERS=
KAFKA_USERNAME=
KAFKA_PASSWORD=
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SSL_CA_PEM=
```

### OpenSearch Values

```text
ELASTICSEARCH_URL=
ELASTICSEARCH_USERNAME=
ELASTICSEARCH_PASSWORD=
ELASTICSEARCH_INDEX=smart-city-unified
```

Important:

```text
ELASTICSEARCH_URL actually points to Aiven OpenSearch.
```

That is okay.

---

## 2. Sign In to Databricks Free Edition

Open:

```text
https://login.databricks.com/signup
```

Choose:

```text
Free Edition
```

Avoid anything that asks for:

```text
AWS account
Azure account
GCP account
Billing
Credit card
```

If you see billing/card screens, you are probably not in the Free Edition path.

---

## 3. Get Your Project Files into Databricks

You have two options.

Use **Option A** if possible.

---

## 3A. Option A — Connect GitHub Repo

In Databricks:

1. Open the left sidebar.
2. Click **Workspace**.
3. Look for **Repos**, **Git folders**, or **Create**.
4. Choose **Add repo** or **Git folder**.
5. Paste your GitHub repo URL.

Example:

```text
https://github.com/<your-username>/smart-city-online.git
```

After adding it, open the folder and confirm you see:

```text
unified_consumer.py
cloud_config.py
requirements.txt
```

If yes, continue to section 4.

---

## 3B. Option B — Upload Files Manually

Use this if GitHub connection is confusing.

In Databricks Workspace:

1. Create a folder named:

```text
smart-city-online
```

2. Upload these files into that folder:

```text
unified_consumer.py
cloud_config.py
```

These two files must be in the **same folder**.

You do not need to upload dashboard files to Databricks.

Databricks only needs:

```text
unified_consumer.py
cloud_config.py
```

---

## 4. Create a Notebook

This is the least confusing way to run the consumer.

In the same folder as your project:

1. Click **Create**.
2. Choose **Notebook**.
3. Name it:

```text
run_unified_consumer
```

4. Language:

```text
Python
```

---

## 5. Attach Compute

At the top of the notebook, Databricks will ask for compute.

Choose the simplest available option:

```text
Serverless
```

or:

```text
Free Edition default compute
```

or:

```text
Create compute
```

If you need to create compute, choose:

```text
Smallest/default option
Single node if available
Latest runtime if asked
```

Do not worry about GPU. You do not need GPU.

---

## 6. Install Python Dependency

In the first notebook cell, paste:

```python
%pip install requests
```

Run the cell.

If Databricks asks to restart Python after install, click restart or run:

```python
dbutils.library.restartPython()
```

Then continue.

---

## 7. Add Spark Kafka Connector

Spark needs the Kafka connector. Without it, you may get:

```text
Failed to find data source: kafka
```

### 7.1 First Try This Notebook Method

In a new notebook cell, paste:

```python
spark.version
```

Run it.

It will output something like:

```text
3.5.0
```

Write down the exact version.

### 7.2 Add Kafka Package in Notebook

In a new cell, paste this, replacing `3.5.0` with your Spark version:

```python
spark.conf.set(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
)
```

Important:

This may not work after Spark has already started. If it does not work, use section 7.3.

### 7.3 Better Method: Add Library to Compute

If notebook config does not work:

1. Open your compute/cluster page.
2. Go to **Libraries**.
3. Click **Install new**.
4. Choose **Maven**.
5. Paste:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:<your-spark-version>
```

Example:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
```

6. Install.
7. Restart compute if Databricks asks.

If you cannot find Libraries in Free Edition, skip for now and run the consumer. If it errors with missing Kafka source, paste the error to Codex.

---

## 8. Paste Environment Variables

Create a new notebook cell.

Paste this template.

Replace all `<...>` values with your Aiven values.

```python
import os

os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "<your-aiven-kafka-host:port>"
os.environ["KAFKA_USERNAME"] = "<your-aiven-kafka-username>"
os.environ["KAFKA_PASSWORD"] = "<your-aiven-kafka-password>"
os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL"
os.environ["KAFKA_SASL_MECHANISM"] = "PLAIN"
os.environ["KAFKA_LOGIN_MODULE"] = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
os.environ["KAFKA_TOPICS"] = "aqi-data,traffic-data,weather-data"
os.environ["KAFKA_STARTING_OFFSETS"] = "latest"
os.environ["STREAM_TRIGGER"] = "availableNow"
os.environ["CHECKPOINT_LOCATION"] = "/Volumes/workspace/default/smart_city_checkpoints/unified_consumer_v2"

os.environ["ELASTICSEARCH_URL"] = "<your-aiven-opensearch-url>"
os.environ["ELASTICSEARCH_USERNAME"] = "<your-aiven-opensearch-username>"
os.environ["ELASTICSEARCH_PASSWORD"] = "<your-aiven-opensearch-password>"
os.environ["ELASTICSEARCH_INDEX"] = "smart-city-unified"
```

If Aiven gave you a Kafka CA certificate, add this too:

```python
os.environ["KAFKA_SSL_CA_PEM"] = """-----BEGIN CERTIFICATE-----
paste your Aiven Kafka CA certificate here
-----END CERTIFICATE-----"""
```

Run this cell.

Expected result:

```text
No output
```

No output is good.

---

## 9. Confirm Files Are Visible

Create a new cell:

```python
import os

print(os.getcwd())
print(os.listdir("."))
```

Run it.

You need to see:

```text
unified_consumer.py
cloud_config.py
```

If you do not see them, your notebook is in the wrong folder.

Fix:

- Move notebook into the same folder as the files, or
- Use the full path to `unified_consumer.py`.

---

## 10. Run the Consumer

Create a new cell:

```python
exec(open("unified_consumer.py").read())
```

Run it.

This cell will keep running because streaming jobs are long-running.

If you set `STREAM_TRIGGER=availableNow`, the cell may finish after processing currently available Kafka records. That is normal on Databricks Serverless/Free Edition.

---

## 10A. If Consumer Shows No Records, Run Kafka Smoke Test

Before debugging the full consumer, prove Databricks can read Kafka.

Run this:

```python
exec(open("databricks_kafka_smoke_test.py").read())
```

Expected output:

```text
Rows by Kafka topic:
+------------+-----+
|topic       |count|
+------------+-----+
|aqi-data    |...  |
|traffic-data|...  |
|weather-data|...  |
+------------+-----+
```

If all counts are missing or zero, the problem is before Spark processing:

```text
Render producers -> Aiven Kafka -> Databricks Kafka read
```

If counts are greater than zero, Kafka is fine and the problem is inside `unified_consumer.py`.

Important: never paste full Kafka config output publicly because it can include your password.

---

## 10B. Recommended Serverless Fix: Run Batch Consumer

If the smoke test shows Kafka rows but the streaming consumer does not write dashboard records, use the batch consumer.

Run:

```python
exec(open("databricks_batch_consumer.py").read())
```

This reads available Kafka records once, joins latest AQI + traffic + weather by zone, writes records to OpenSearch, and exits.

Expected success:

```text
Kafka rows by topic:
...
Indexed 5 records into smart-city-unified.
```

For a free demo, this is enough. Rerun the same cell whenever you want to refresh dashboard data.

---

## 11. What Successful Logs Look Like

Good startup:

```text
Unified Multi-Topic Consumer Online
```

Good processing:

```text
Unified Batch: 1
Indexed 5 records into smart-city-unified.
```

The first minute may show:

```text
No Kafka records received in this trigger.
```

That is okay briefly.

If it keeps saying that for several minutes, Kafka is not being read.

---

## 12. When to Open Dashboard

Only open/check dashboard after Databricks logs show:

```text
Indexed 5 records into smart-city-unified.
```

Then open:

```text
https://smart-city-online.onrender.com
```

Also test:

```text
https://smart-city-online.onrender.com/api/summary
```

---

## 13. Common Errors and Exact Meaning

### Error: Failed to find data source: kafka

Meaning:

```text
Spark Kafka connector is missing.
```

Fix:

Add Maven library:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:<your-spark-version>
```

---

### Error: Unable to connect to Kafka

Meaning:

```text
Kafka env vars are wrong, certificate missing, or Aiven Kafka is unreachable.
```

Check:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_USERNAME
KAFKA_PASSWORD
KAFKA_SSL_CA_PEM
```

Also check Render producers are still running and Aiven Kafka service is running.

---

### Error: No LoginModule found for PlainLoginModule

Meaning:

```text
Databricks Kafka connector expects the shaded Kafka login module class.
```

Fix:

Set this environment variable before running `unified_consumer.py`:

```python
os.environ["KAFKA_LOGIN_MODULE"] = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
```

Then rerun:

```python
exec(open("unified_consumer.py").read())
```

---

### Error: Elasticsearch request failed

Meaning:

```text
OpenSearch URL/auth is wrong, or OpenSearch is unreachable.
```

Check:

```text
ELASTICSEARCH_URL
ELASTICSEARCH_USERNAME
ELASTICSEARCH_PASSWORD
```

---

### Error: No module named cloud_config

Meaning:

```text
cloud_config.py is not in the same folder as unified_consumer.py.
```

Fix:

Upload/move `cloud_config.py` next to `unified_consumer.py`.

---

### Error: No such file unified_consumer.py

Meaning:

```text
Notebook is not running in the folder containing unified_consumer.py.
```

Fix:

Run:

```python
print(os.getcwd())
print(os.listdir("."))
```

Then move notebook or use the correct file path.

---

### Error: SparkContext is not supported on serverless compute

Meaning:

```text
The code tried to access spark.sparkContext, which Databricks Serverless blocks.
```

Fix:

```text
Pull the latest code from GitHub and rerun unified_consumer.py.
```

The project now skips SparkContext log-level setup when Serverless does not allow it.

If you still see this error after pulling the latest code, use non-serverless compute with:

```text
Dedicated access mode
```

---

### Error: ProcessingTime is not supported for this cluster type

Meaning:

```text
Databricks Serverless/Free Edition does not allow an infinite processingTime streaming trigger.
```

Fix:

Set this environment variable before running `unified_consumer.py`:

```python
os.environ["STREAM_TRIGGER"] = "availableNow"
```

Also confirm Databricks sees it:

```python
import os
print(os.environ.get("STREAM_TRIGGER"))
```

Expected output:

```text
availableNow
```

Then rerun:

```python
exec(open("unified_consumer.py").read())
```

With `availableNow`, Databricks processes the Kafka records currently available and then stops. To process more records later, run the cell again.

---

### Error: temporary streaming checkpoint locations are not supported

Meaning:

```text
Databricks Serverless requires you to provide a checkpoint location for streaming.
```

Fix:

Set this environment variable before running `unified_consumer.py`:

```python
os.environ["CHECKPOINT_LOCATION"] = "/Volumes/workspace/default/smart_city_checkpoints/unified_consumer"
```

Then rerun:

```python
exec(open("unified_consumer.py").read())
```

If you rerun from scratch and want Databricks to reprocess old Kafka records, use a new checkpoint path:

```python
os.environ["CHECKPOINT_LOCATION"] = "/Volumes/workspace/default/smart_city_checkpoints/unified_consumer_v2"
```

---

### Error: Public DBFS root is disabled

Meaning:

```text
Your workspace blocks dbfs:/tmp checkpoints.
```

Databricks recommends checkpoint locations in Unity Catalog Volumes, so use a path beginning with:

```text
/Volumes/
```

First create a volume by running this in a SQL notebook/cell:

```sql
CREATE VOLUME IF NOT EXISTS workspace.default.smart_city_checkpoints;
```

Then set this in your Python env-var cell:

```python
os.environ["CHECKPOINT_LOCATION"] = "/Volumes/workspace/default/smart_city_checkpoints/unified_consumer"
```

If `workspace.default` does not exist, run:

```sql
SHOW CATALOGS;
```

Use one of the listed catalogs instead of `workspace`, then create/use a schema and volume there.

---

## 14. If Notebook Works, You Are Done

For a demo, leaving the notebook running is enough.

Your final online system is:

```text
Render producers are sending data
Databricks notebook is processing data
OpenSearch is storing records
Render dashboard is showing records
```

---

## 15. Optional Later: Convert Notebook to Job

Only do this after the notebook works.

In Databricks:

1. Go to **Workflows**.
2. Create Job.
3. Task type:

```text
Notebook
```

4. Select notebook:

```text
run_unified_consumer
```

5. Use the same compute.
6. Run job.

Do not start with Jobs. Start with notebook. Less chaos, fewer buttons.

---

## 16. Tiny Checklist

Use this while doing Databricks:

```text
[ ] Databricks Free Edition opened
[ ] Repo/files visible
[ ] Notebook created
[ ] Compute attached
[ ] requests installed
[ ] Spark version checked
[ ] Kafka connector added if needed
[ ] Env vars pasted
[ ] unified_consumer.py visible
[ ] Consumer cell started
[ ] Logs show Unified Multi-Topic Consumer Online
[ ] Logs show Indexed 5 records
[ ] Dashboard shows data
```

---

## 17. If You Feel Lost

Stop and check only these three things:

```text
1. Is Render producing messages to Aiven Kafka?
2. Is Databricks reading from Aiven Kafka?
3. Is Databricks writing to Aiven OpenSearch?
```

Do not debug the dashboard until Databricks logs show:

```text
Indexed 5 records into smart-city-unified.
```
