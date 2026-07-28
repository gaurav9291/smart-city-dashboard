import json
import os
import requests
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from cloud_config import (
    get_elasticsearch_auth,
    get_elasticsearch_index,
    get_elasticsearch_url,
    get_spark_kafka_options,
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, trim, lower, row_number, desc
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType
)

# 1. Initialize Spark Session.
# Kafka connector packages should be supplied by spark-submit --packages.
spark = SparkSession.builder \
    .appName("SmartCity-Unified-Stream") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

try:
    spark.sparkContext.setLogLevel("WARN")
except Exception as exc:
    print(f"SparkContext log-level setup skipped: {exc}")

ELASTICSEARCH_URL = get_elasticsearch_url()
ELASTICSEARCH_INDEX = get_elasticsearch_index()
ELASTICSEARCH_AUTH = get_elasticsearch_auth()
ELASTICSEARCH_TIMEOUT_SECONDS = 30
ELASTICSEARCH_MAX_RETRIES = 3
ELASTICSEARCH_RETRY_BACKOFF_SECONDS = 2
STREAM_TRIGGER = os.getenv("STREAM_TRIGGER", "processingTime").strip().lower()
STREAM_PROCESSING_TIME = os.getenv("STREAM_PROCESSING_TIME", "30 seconds")
LOCAL_TIMEZONE = ZoneInfo("Asia/Kolkata")
EXPECTED_ZONES = ["fc road", "hadapsar", "hinjewadi", "kothrud", "shivajinagar"]
LATEST_AQI_BY_ZONE = {}
LATEST_TRAFFIC_BY_ZONE = {}
LATEST_WEATHER_BY_ZONE = {}
ELASTICSEARCH_INDEX_TEMPLATE = {
    "index_patterns": [f"{ELASTICSEARCH_INDEX}*"],
    "template": {
        "settings": {
            "number_of_replicas": 0,
            "refresh_interval": "30s"
        }
    }
}

# 2. Tight Schemas matching your standalone producer scripts exactly
aqi_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("aqi", IntegerType(), True),  # Matches your producer payload key
    StructField("pm25", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("co", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

traffic_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("current_speed_kmph", DoubleType(), True),
    StructField("free_flow_speed", IntegerType(), True),
    StructField("congestion_pct", DoubleType(), True),
    StructField("road_closure", BooleanType(), True),
    StructField("data_source", StringType(), True)
])

weather_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("wind_speed_kmph", DoubleType(), True),
    StructField("humidity_pct", IntegerType(), True),
    StructField("rainfall_mm", DoubleType(), True),
    StructField("pressure_hpa", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# 3. Kafka Multi-Topic Reader Ingestion Engine
raw_stream = spark.readStream \
    .format("kafka") \
    .options(**get_spark_kafka_options()) \
    .load() \
    .selectExpr(
        "topic",
        "CAST(value AS STRING) as json_payload",
        "timestamp as kafka_time"
    )

# ─────────────────────────────────────────────────────────────
# 4. Per-Batch Parsing + Latest Row Per Zone
# ─────────────────────────────────────────────────────────────
def latest_by_zone(df, time_col):
    zone_window = Window.partitionBy("norm_zone").orderBy(desc(time_col))
    return df.withColumn("rn", row_number().over(zone_window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

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
    try:
        response = requests.put(
            f"{ELASTICSEARCH_URL}/_index_template/{ELASTICSEARCH_INDEX}-template",
            json=ELASTICSEARCH_INDEX_TEMPLATE,
            auth=ELASTICSEARCH_AUTH,
            timeout=ELASTICSEARCH_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        print(f"Elasticsearch index template ready for {ELASTICSEARCH_INDEX}*.")
    except requests.RequestException as exc:
        print(f"Elasticsearch template setup skipped: {exc}")

def log_bulk_item_errors(result, batch_id):
    failed_items = []
    for item in result.get("items", []):
        action = item.get("index", {})
        if action.get("error"):
            failed_items.append({
                "status": action.get("status"),
                "id": action.get("_id"),
                "error": action.get("error")
            })

    print(f"Elasticsearch bulk write had {len(failed_items)} item errors in batch {batch_id}.")
    for item in failed_items[:5]:
        print(f"Bulk item error: {json.dumps(item)}")
    if len(failed_items) > 5:
        print(f"... {len(failed_items) - 5} more bulk item errors omitted.")

def write_to_elasticsearch(df, batch_id):
    rows = df.collect()
    if not rows:
        print("No Elasticsearch documents to write for this trigger.")
        return False

    bulk_lines = []
    for row in rows:
        doc = normalize_document(row)
        doc_id = f"{doc['zone']}-{doc['synchronized_time']}"
        bulk_lines.append(json.dumps({
            "index": {
                "_index": ELASTICSEARCH_INDEX,
                "_id": doc_id
            }
        }))
        bulk_lines.append(json.dumps(doc))

    payload = "\n".join(bulk_lines) + "\n"
    for attempt in range(1, ELASTICSEARCH_MAX_RETRIES + 1):
        try:
            response = requests.post(
                f"{ELASTICSEARCH_URL}/_bulk?refresh=false",
                data=payload,
                headers={"Content-Type": "application/x-ndjson"},
                auth=ELASTICSEARCH_AUTH,
                timeout=ELASTICSEARCH_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            result = response.json()
        except requests.RequestException as exc:
            if attempt < ELASTICSEARCH_MAX_RETRIES:
                print(
                    f"Elasticsearch write failed for batch {batch_id} "
                    f"(attempt {attempt}/{ELASTICSEARCH_MAX_RETRIES}): {exc}. Retrying..."
                )
                time.sleep(ELASTICSEARCH_RETRY_BACKOFF_SECONDS * attempt)
                continue

            print(
                f"Elasticsearch write failed for batch {batch_id} after "
                f"{ELASTICSEARCH_MAX_RETRIES} attempts: {exc}. Stream will continue."
            )
            return False
        except ValueError as exc:
            print(f"Elasticsearch returned invalid JSON for batch {batch_id}: {exc}. Stream will continue.")
            return False

        if result.get("errors"):
            log_bulk_item_errors(result, batch_id)
            return False

        print(f"Indexed {len(rows)} records into {ELASTICSEARCH_INDEX}.")
        return True

    return False

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

def update_latest_cache(df, cache):
    for row in df.collect():
        doc = normalize_document(row)
        cache[doc["norm_zone"]] = doc

def build_cached_unified_documents():
    complete_zones = [
        zone for zone in EXPECTED_ZONES
        if zone in LATEST_AQI_BY_ZONE
        and zone in LATEST_TRAFFIC_BY_ZONE
        and zone in LATEST_WEATHER_BY_ZONE
    ]

    documents = []
    for norm_zone in complete_zones:
        aqi_doc = LATEST_AQI_BY_ZONE[norm_zone]
        traffic_doc = LATEST_TRAFFIC_BY_ZONE[norm_zone]
        weather_doc = LATEST_WEATHER_BY_ZONE[norm_zone]

        synchronized_time = max(
            aqi_doc["aqi_kafka_time"],
            traffic_doc["traffic_kafka_time"],
            weather_doc["weather_kafka_time"]
        )

        documents.append({
            "zone": aqi_doc["zone"],
            "synchronized_time": synchronized_time,
            "aqi_sensor_id": aqi_doc["aqi_sensor_id"],
            "aqi_lat": aqi_doc["aqi_lat"],
            "aqi_lon": aqi_doc["aqi_lon"],
            "aqi_source_timestamp": aqi_doc["aqi_source_timestamp"],
            "aqi_kafka_time": aqi_doc["aqi_kafka_time"],
            "aqi": aqi_doc["aqi"],
            "aqi_status": aqi_status_for(aqi_doc["aqi"]),
            "pm25": aqi_doc["pm25"],
            "pm10": aqi_doc["pm10"],
            "no2": aqi_doc["no2"],
            "co": aqi_doc["co"],
            "aqi_temperature": aqi_doc["aqi_temperature"],
            "aqi_humidity": aqi_doc["aqi_humidity"],
            "aqi_wind": aqi_doc["aqi_wind"],
            "traffic_sensor_id": traffic_doc["traffic_sensor_id"],
            "traffic_zone": traffic_doc["traffic_zone"],
            "traffic_lat": traffic_doc["traffic_lat"],
            "traffic_lon": traffic_doc["traffic_lon"],
            "traffic_source_timestamp": traffic_doc["traffic_source_timestamp"],
            "traffic_kafka_time": traffic_doc["traffic_kafka_time"],
            "current_speed_kmph": traffic_doc["current_speed_kmph"],
            "free_flow_speed": traffic_doc["free_flow_speed"],
            "congestion_pct": traffic_doc["congestion_pct"],
            "road_closure": traffic_doc["road_closure"],
            "traffic_data_source": traffic_doc["traffic_data_source"],
            "traffic_status": traffic_status_for(
                traffic_doc["road_closure"],
                traffic_doc["congestion_pct"]
            ),
            "weather_sensor_id": weather_doc["weather_sensor_id"],
            "weather_zone": weather_doc["weather_zone"],
            "weather_lat": weather_doc["weather_lat"],
            "weather_lon": weather_doc["weather_lon"],
            "weather_source_timestamp": weather_doc["weather_source_timestamp"],
            "weather_kafka_time": weather_doc["weather_kafka_time"],
            "temperature_c": weather_doc["temperature_c"],
            "wind_speed_kmph": weather_doc["wind_speed_kmph"],
            "humidity_pct": weather_doc["humidity_pct"],
            "rainfall_mm": weather_doc["rainfall_mm"],
            "pressure_hpa": weather_doc["pressure_hpa"],
            "weather_condition": weather_condition_for(
                weather_doc["rainfall_mm"],
                weather_doc["temperature_c"]
            ),
            "city_alert": city_alert_for(
                aqi_doc["aqi"],
                traffic_doc["congestion_pct"],
                traffic_doc["road_closure"]
            )
        })

    return documents

def process_batch(batch_df, batch_id):
    print(f"\n-------------------------------------------")
    print(f"Unified Batch: {batch_id}")
    print("-------------------------------------------")

    if batch_df.isEmpty():
        print("No Kafka records received in this trigger.")
        return

    aqi_batch = batch_df.filter(col("topic") == "aqi-data") \
        .select(from_json(col("json_payload"), aqi_schema).alias("d"), col("kafka_time")) \
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
            col("kafka_time").alias("aqi_kafka_time")
        )

    traffic_batch = batch_df.filter(col("topic") == "traffic-data") \
        .select(from_json(col("json_payload"), traffic_schema).alias("d"), col("kafka_time")) \
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
            col("kafka_time").alias("traffic_kafka_time")
        )

    weather_batch = batch_df.filter(col("topic") == "weather-data") \
        .select(from_json(col("json_payload"), weather_schema).alias("d"), col("kafka_time")) \
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
            col("kafka_time").alias("weather_kafka_time")
        )

    latest_aqi = latest_by_zone(aqi_batch, "aqi_kafka_time")
    latest_traffic = latest_by_zone(traffic_batch, "traffic_kafka_time")
    latest_weather = latest_by_zone(weather_batch, "weather_kafka_time")

    update_latest_cache(latest_aqi, LATEST_AQI_BY_ZONE)
    update_latest_cache(latest_traffic, LATEST_TRAFFIC_BY_ZONE)
    update_latest_cache(latest_weather, LATEST_WEATHER_BY_ZONE)

    documents = build_cached_unified_documents()
    if not documents:
        print("Waiting for at least one complete AQI + traffic + weather zone.")
        return

    analyzed = spark.createDataFrame(documents) \
        .select(
            "zone", "synchronized_time",
            "aqi_sensor_id", "aqi_lat", "aqi_lon", "aqi_source_timestamp",
            "aqi_kafka_time", "aqi", "aqi_status", "pm25", "pm10", "no2",
            "co", "aqi_temperature", "aqi_humidity", "aqi_wind",
            "traffic_sensor_id", "traffic_zone", "traffic_lat", "traffic_lon",
            "traffic_source_timestamp", "traffic_kafka_time",
            "current_speed_kmph", "free_flow_speed", "congestion_pct",
            "road_closure", "traffic_data_source", "traffic_status",
            "weather_sensor_id", "weather_zone", "weather_lat", "weather_lon",
            "weather_source_timestamp", "weather_kafka_time",
            "temperature_c", "wind_speed_kmph", "humidity_pct", "rainfall_mm",
            "pressure_hpa", "weather_condition", "city_alert"
        ) \
        .orderBy("zone")

    if len(documents) < len(EXPECTED_ZONES):
        print(f"State cache has {len(documents)} complete zones; still warming up to 5.")

    analyzed.show(100, truncate=False, vertical=True)
    write_to_elasticsearch(analyzed, batch_id)

# 5. Output one latest joined row per zone for each micro-batch.
install_elasticsearch_index_template()

query_builder = raw_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append")

if STREAM_TRIGGER in {"availablenow", "available_now"}:
    query_builder = query_builder.trigger(availableNow=True)
elif STREAM_TRIGGER == "once":
    query_builder = query_builder.trigger(once=True)
else:
    query_builder = query_builder.trigger(processingTime=STREAM_PROCESSING_TIME)

query = query_builder.start()

print("🏁 Unified Multi-Topic Consumer Online. Emitting latest joined row per zone per batch...")
query.awaitTermination()
