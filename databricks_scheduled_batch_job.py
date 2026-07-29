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
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "smart-city-kafka-gaurav4pf-eb89.j.aivencloud.com:28611"
os.environ["KAFKA_USERNAME"] = "avnadmin"
os.environ["KAFKA_PASSWORD"] = "AVNS_Q45bXH1F4GDTQDBKWl-"
os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL"
os.environ["KAFKA_SASL_MECHANISM"] = "PLAIN"
os.environ[
    "KAFKA_LOGIN_MODULE",
] = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
os.environ["KAFKA_TOPICS"] = "aqi-data,traffic-data,weather-data"
os.environ["BATCH_STARTING_OFFSETS"] = "earliest"


# COMMAND ----------

# OpenSearch / Aiven settings.
# Use the clean host URL, not a URL containing username/password.
os.environ["ELASTICSEARCH_URL"] = "https://smart-city-opensearch-gaurav4pf-eb89.i.aivencloud.com:28598"
os.environ["ELASTICSEARCH_USERNAME"] = "avnadmin"
os.environ["ELASTICSEARCH_PASSWORD"] = "AVNS_4B65ILvcEWQ0Brw_4yO"
os.environ["ELASTICSEARCH_INDEX"] = "smart-city-unified"


# COMMAND ----------

os.environ["KAFKA_SSL_CA_PEM"] = """-----BEGIN CERTIFICATE-----
MIIERDCCAqygAwIBAgIUBp0duzTRgZCN7yW9DvNG6uM8KOcwDQYJKoZIhvcNAQEM
BQAwOjE4MDYGA1UEAwwvMTQxMGMxM2UtNzM4Yy00MTA0LThjZGEtODRhMjZlMzZl
NjE4IFByb2plY3QgQ0EwHhcNMjYwNzI4MDgzNzUwWhcNMzYwNzI1MDgzNzUwWjA6
MTgwNgYDVQQDDC8xNDEwYzEzZS03MzhjLTQxMDQtOGNkYS04NGEyNmUzNmU2MTgg
UHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCCAYoCggGBAJr0iIm/
o8kpTsSW5cu81S5ceai6ujz5EY+E3i0IutVDm1TzfPuMOMG4mMADEymU3Q/LP0ct
p4d/1Y7Lq3YR/jLDJz6p8bXRqbjCbYZEEuXhCDbimBLKgSC7Tph9HEPZ8mehepWW
fcORsoEyrtzFwrqqwoxYmmY3p8qOQ1EoCLl+eVmBgUdjgK2kiBm70Q4ou3hhqNkA
xoZSMgag8nYT6WqyqTRU+ReD1hVnyzHMVcgY127YatKyRZRIZYdnEqghuiuaptMN
pq9AxUZZYHxhJNpMM4LWvWUCTZpgdqMMilsluIxi9N7z73P2nY0iavcAn+7PBUJR
5rF4b62WqOoEaUHlr48P5p12aq2JsR4vRfFgk3nHXDluauTgaiY4N4Pwkc+ty0a4
y0H7UNpBHfjU+mIPC6krKdAabz2QKt0uIoqk15g9mgTVU69mXE4XAGVYHFhBkzXM
D/xFVbj8ReMRGHj8dB+QeZ93DwtV9O5IyKfWKPIrn9FnnK4znJF5Qcy26wIDAQAB
o0IwQDAdBgNVHQ4EFgQURBFw9Bj2p05K9/es9BoOE7ZSi5MwEgYDVR0TAQH/BAgw
BgEB/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQADggGBAHS/p0BwU3YD
wvcTYxa7OfJgM54TLK+c3nOa7XM/Ouy3YAoSFhs660vOC6DejjfzKqiQubXxVQC6
9UQbGUFw8QpXskuDVGkWv+zEx/Lx6RkD3qjSroQg/7sP0gPjfGEk38InRyvjjXb4
G+yHntt2JxWpzHkp4jwyYqcw2WWU61q7ZHpf4u90e96/Dj6BI87EDzJngfJHOKLU
T+KrOqQglFWzyLaOj1T9PNjg3S3rB9Ks6S2dEpOwdhBkCisL/cKprcc8a9FEwc//
A3dl/zhwle7Rtd9ynDIjk7LrJ3vBFchf/NTlOHtLLYHSCNGl6UTFvwRmty+PfZIw
9qX9fyxXgPpw49jBinBWjqV4dYEYVyBCqAtyTsVUU/aw7ej/2E7eUrIUV5E0aUeT
YOssr83+BQ/NwrjziLOOSqGyxOpTvl/KrMI+vWiKfihmw0nqkCLPmdY4++Sf9Drl
8z/zHE3UP8w5oSin4dYlZjGQVV6GvQ26n0jCMIkCjxj+L/Hp9bqf8g==
-----END CERTIFICATE-----"""


# COMMAND ----------

# Keep short dashboard filters useful after each scheduled run.
os.environ["BATCH_SYNCHRONIZED_TIME_MODE"] = "processing_time"


# COMMAND ----------

exec(open("databricks_batch_consumer.py").read())
