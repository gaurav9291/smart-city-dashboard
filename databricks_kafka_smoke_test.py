from pyspark.sql.functions import col

from cloud_config import get_spark_kafka_options


options = get_spark_kafka_options()
options["startingOffsets"] = "earliest"
options["endingOffsets"] = "latest"

print("Kafka smoke test options:")
for key, value in options.items():
    if "password" in key.lower() or "jaas" in key.lower() or "certificates" in key.lower():
        print(f"{key}=***")
    else:
        print(f"{key}={value}")

df = spark.read.format("kafka").options(**options).load()

print("Rows by Kafka topic:")
df.groupBy("topic").count().show(truncate=False)

print("Sample Kafka messages:")
df.select(
    col("topic"),
    col("timestamp"),
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
).show(20, truncate=False)
