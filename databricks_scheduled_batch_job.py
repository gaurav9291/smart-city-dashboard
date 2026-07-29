# Databricks notebook source
# Scheduled Databricks runner for the Smart City dashboard refresh.
#
# Put this notebook in the same Databricks Git folder as:
# - databricks_batch_consumer.py
# - cloud_config.py
#
# Then create a Databricks Job that runs this notebook every 5-15 minutes.

# COMMAND ----------

import os


# COMMAND ----------

# Kafka / Aiven settings.
# Fill these in inside Databricks, or provide them through Databricks job/compute env vars.
# Do not commit real passwords to Git.
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "<your-aiven-kafka-host:port>")
os.environ.setdefault("KAFKA_USERNAME", "<your-aiven-kafka-username>")
os.environ.setdefault("KAFKA_PASSWORD", "<your-aiven-kafka-password>")
os.environ.setdefault("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
os.environ.setdefault("KAFKA_SASL_MECHANISM", "PLAIN")
os.environ.setdefault(
    "KAFKA_LOGIN_MODULE",
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule",
)
os.environ.setdefault("KAFKA_TOPICS", "aqi-data,traffic-data,weather-data")
os.environ.setdefault("BATCH_STARTING_OFFSETS", "earliest")


# COMMAND ----------

# OpenSearch / Aiven settings.
# Use the clean host URL, not a URL containing username/password.
os.environ.setdefault("ELASTICSEARCH_URL", "<your-aiven-opensearch-url>")
os.environ.setdefault("ELASTICSEARCH_USERNAME", "<your-aiven-opensearch-username>")
os.environ.setdefault("ELASTICSEARCH_PASSWORD", "<your-aiven-opensearch-password>")
os.environ.setdefault("ELASTICSEARCH_INDEX", "smart-city-unified")


# COMMAND ----------

# Optional Kafka CA certificate.
# If Aiven requires this, uncomment and paste the real certificate in Databricks:
#
# os.environ["KAFKA_SSL_CA_PEM"] = """-----BEGIN CERTIFICATE-----
# paste your Aiven Kafka CA certificate here
# -----END CERTIFICATE-----"""


# COMMAND ----------

# Keep short dashboard filters useful after each scheduled run.
os.environ.setdefault("BATCH_SYNCHRONIZED_TIME_MODE", "processing_time")


# COMMAND ----------

required_env_vars = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_USERNAME",
    "KAFKA_PASSWORD",
    "ELASTICSEARCH_URL",
    "ELASTICSEARCH_USERNAME",
    "ELASTICSEARCH_PASSWORD",
]

missing_or_placeholder = [
    name
    for name in required_env_vars
    if not os.environ.get(name) or os.environ[name].startswith("<")
]

if missing_or_placeholder:
    raise RuntimeError(
        "Configure these Databricks environment variables before scheduling this job: "
        + ", ".join(missing_or_placeholder)
    )


# COMMAND ----------

exec(open("databricks_batch_consumer.py").read())
