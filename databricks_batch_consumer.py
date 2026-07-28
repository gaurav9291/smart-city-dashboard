import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, from_json, lower, row_number, trim
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from cloud_config import (
    get_elasticsearch_auth,
    get_elasticsearch_index,
    get_elasticsearch_url,
    get_spark_kafka_options,
)


spark = SparkSession.builder.appName("SmartCity-Batch-Kafka-Consumer").getOrCreate()

ELASTICSEARCH_URL = get_elasticsearch_url()
ELASTICSEARCH_INDEX = get_elasticsearch_index()
ELASTICSEARCH_AUTH = get_elasticsearch_auth()
ELASTICSEARCH_TIMEOUT_SECONDS = 30
ELASTICSEARCH_MAX_RETRIES = 3
ELASTICSEARCH_RETRY_BACKOFF_SECONDS = 2
LOCAL_TIMEZONE = ZoneInfo("Asia/Kolkata")

ELASTICSEARCH_INDEX_TEMPLATE = {
    "index_patterns": [f"{ELASTICSEARCH_INDEX}*"],
    "template": {
        "settings": {
            "number_of_replicas": 0,
            "refresh_interval": "30s",
        }
    },
}

aqi_schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("co", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("wind", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

traffic_schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("current_speed_kmph", DoubleType(), True),
        StructField("free_flow_speed", IntegerType(), True),
        StructField("congestion_pct", DoubleType(), True),
        StructField("road_closure", BooleanType(), True),
        StructField("data_source", StringType(), True),
    ]
)

weather_schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("wind_speed_kmph", DoubleType(), True),
        StructField("humidity_pct", IntegerType(), True),
        StructField("rainfall_mm", DoubleType(), True),
        StructField("pressure_hpa", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


def latest_by_zone(df, time_col):
    zone_window = Window.partitionBy("norm_zone").orderBy(desc(time_col))
    return df.withColumn("rn", row_number().over(zone_window)).filter(col("rn") == 1).drop("rn")


def normalize_document(row):
    doc = row.asDict(recursive=True)
    for key, value in list(doc.items()):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=LOCAL_TIMEZONE)
            doc[key] = value.isoformat()
        elif hasattr(value, "isoformat"):
            doc[key] = value.isoformat()
    return doc


def install_elasticsearch_index_template():
    response = requests.put(
        f"{ELASTICSEARCH_URL}/_index_template/{ELASTICSEARCH_INDEX}-template",
        json=ELASTICSEARCH_INDEX_TEMPLATE,
        auth=ELASTICSEARCH_AUTH,
        timeout=ELASTICSEARCH_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    print(f"OpenSearch index template ready for {ELASTICSEARCH_INDEX}*.")


def write_to_elasticsearch(df):
    rows = df.collect()
    if not rows:
        print("No documents to write.")
        return

    bulk_lines = []
    for row in rows:
        doc = normalize_document(row)
        doc_id = f"{doc['zone']}-{doc['synchronized_time']}"
        bulk_lines.append(json.dumps({"index": {"_index": ELASTICSEARCH_INDEX, "_id": doc_id}}))
        bulk_lines.append(json.dumps(doc))

    payload = "\n".join(bulk_lines) + "\n"
    for attempt in range(1, ELASTICSEARCH_MAX_RETRIES + 1):
        response = requests.post(
            f"{ELASTICSEARCH_URL}/_bulk?refresh=true",
            data=payload,
            headers={"Content-Type": "application/x-ndjson"},
            auth=ELASTICSEARCH_AUTH,
            timeout=ELASTICSEARCH_TIMEOUT_SECONDS,
        )
        if response.ok:
            result = response.json()
            if result.get("errors"):
                raise RuntimeError(f"OpenSearch bulk item errors: {json.dumps(result)[:1000]}")
            print(f"Indexed {len(rows)} records into {ELASTICSEARCH_INDEX}.")
            return

        if attempt < ELASTICSEARCH_MAX_RETRIES:
            print(f"OpenSearch write failed, retrying: {response.status_code} {response.text[:300]}")
            time.sleep(ELASTICSEARCH_RETRY_BACKOFF_SECONDS * attempt)
            continue

        response.raise_for_status()


def aqi_status_for(aqi):
    if aqi > 300:
        return "Hazardous"
    if aqi > 200:
        return "Severe"
    if aqi > 100:
        return "Unhealthy"
    return "Acceptable"


def traffic_status_for(road_closure, congestion_pct):
    if road_closure:
        return "BLOCKAGE"
    if congestion_pct > 60.0:
        return "HEAVY CONGESTION"
    if congestion_pct > 30.0:
        return "MODERATE TRAFFIC"
    return "FREE FLOW"


def weather_condition_for(rainfall_mm, temperature_c):
    if rainfall_mm > 5.0:
        return "Heavy Rain"
    if temperature_c > 38.0:
        return "Extreme Heat"
    return "Normal"


def city_alert_for(aqi, congestion_pct, road_closure):
    if aqi > 200 and congestion_pct > 60.0:
        return "🔴 POLLUTION TRAP"
    if road_closure:
        return "⚠️ ROAD CLOSED"
    return "🟢 ALL CLEAR"


kafka_options = get_spark_kafka_options()
kafka_options["startingOffsets"] = os.getenv("BATCH_STARTING_OFFSETS", "earliest")
kafka_options["endingOffsets"] = "latest"

print("Reading available Kafka records once...")
raw_df = (
    spark.read.format("kafka")
    .options(**kafka_options)
    .load()
    .selectExpr("topic", "CAST(value AS STRING) as json_payload", "timestamp as kafka_time")
)

print("Kafka rows by topic:")
raw_df.groupBy("topic").count().show(truncate=False)

aqi_latest = latest_by_zone(
    raw_df.filter(col("topic") == "aqi-data")
    .select(from_json(col("json_payload"), aqi_schema).alias("d"), col("kafka_time"))
    .select(
        lower(trim(col("d.zone"))).alias("norm_zone"),
        col("d.sensor_id").alias("aqi_sensor_id"),
        col("d.zone").alias("zone"),
        col("d.lat").alias("aqi_lat"),
        col("d.lon").alias("aqi_lon"),
        col("d.aqi").alias("aqi"),
        col("d.pm25").alias("pm25"),
        col("d.pm10").alias("pm10"),
        col("d.no2").alias("no2"),
        col("d.co").alias("co"),
        col("d.temperature").alias("aqi_temperature"),
        col("d.humidity").alias("aqi_humidity"),
        col("d.wind").alias("aqi_wind"),
        col("d.timestamp").alias("aqi_source_timestamp"),
        col("kafka_time").alias("aqi_kafka_time"),
    ),
    "aqi_kafka_time",
)

traffic_latest = latest_by_zone(
    raw_df.filter(col("topic") == "traffic-data")
    .select(from_json(col("json_payload"), traffic_schema).alias("d"), col("kafka_time"))
    .select(
        lower(trim(col("d.zone"))).alias("norm_zone"),
        col("d.sensor_id").alias("traffic_sensor_id"),
        col("d.zone").alias("traffic_zone"),
        col("d.lat").alias("traffic_lat"),
        col("d.lon").alias("traffic_lon"),
        col("d.timestamp").alias("traffic_source_timestamp"),
        col("d.current_speed_kmph").alias("current_speed_kmph"),
        col("d.free_flow_speed").alias("free_flow_speed"),
        col("d.congestion_pct").alias("congestion_pct"),
        col("d.road_closure").alias("road_closure"),
        col("d.data_source").alias("traffic_data_source"),
        col("kafka_time").alias("traffic_kafka_time"),
    ),
    "traffic_kafka_time",
)

weather_latest = latest_by_zone(
    raw_df.filter(col("topic") == "weather-data")
    .select(from_json(col("json_payload"), weather_schema).alias("d"), col("kafka_time"))
    .select(
        lower(trim(col("d.zone"))).alias("norm_zone"),
        col("d.sensor_id").alias("weather_sensor_id"),
        col("d.zone").alias("weather_zone"),
        col("d.lat").alias("weather_lat"),
        col("d.lon").alias("weather_lon"),
        col("d.temperature_c").alias("temperature_c"),
        col("d.wind_speed_kmph").alias("wind_speed_kmph"),
        col("d.humidity_pct").alias("humidity_pct"),
        col("d.rainfall_mm").alias("rainfall_mm"),
        col("d.pressure_hpa").alias("pressure_hpa"),
        col("d.timestamp").alias("weather_source_timestamp"),
        col("kafka_time").alias("weather_kafka_time"),
    ),
    "weather_kafka_time",
)

joined = (
    aqi_latest.join(traffic_latest, "norm_zone", "inner")
    .join(weather_latest, "norm_zone", "inner")
)

documents = []
for row in joined.collect():
    doc = normalize_document(row)
    synchronized_time = max(
        doc["aqi_kafka_time"],
        doc["traffic_kafka_time"],
        doc["weather_kafka_time"],
    )
    documents.append(
        {
            **doc,
            "zone": doc["zone"],
            "synchronized_time": synchronized_time,
            "aqi_status": aqi_status_for(doc["aqi"]),
            "traffic_status": traffic_status_for(doc["road_closure"], doc["congestion_pct"]),
            "weather_condition": weather_condition_for(doc["rainfall_mm"], doc["temperature_c"]),
            "city_alert": city_alert_for(doc["aqi"], doc["congestion_pct"], doc["road_closure"]),
        }
    )

if not documents:
    raise RuntimeError("No complete AQI + traffic + weather zone records were found.")

final_df = spark.createDataFrame(documents).orderBy("zone")
final_df.show(100, truncate=False, vertical=True)

install_elasticsearch_index_template()
write_to_elasticsearch(final_df)
