from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

# 1. Initialize Spark Session configured for your local Spark 4 development cluster
spark = SparkSession.builder \
    .appName("SmartCity-Traffic-Stream") \
    .getOrCreate()

# Suppress verbose verbose info logs to keep console clean
spark.sparkContext.setLogLevel("WARN")

# 2. Define schema matching our producer's payload keys explicitly
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

# 3. Connect to the Kafka bootstrap server and subscribe to the topic
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Deserialize JSON bytes from the raw Kafka value into clear columns
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), traffic_schema).alias("data")) \
    .select("data.*")

# 5. Add transformation logic (Rule-based operational tagging)
analyzed_traffic = parsed_stream.withColumn(
    "traffic_status",
    when(col("road_closure") == True, "BLOCKAGE")
    .when(col("congestion_pct") > 60.0, "HEAVY CONGESTION")
    .when(col("congestion_pct") > 30.0, "MODERATE TRAFFIC")
    .otherwise("FREE FLOW")
).withColumn(
    "is_alert",
    when((col("road_closure") == True) | (col("current_speed_kmph") < 20.0), True).otherwise(False)
)

# 6. Output stream directly to Console with our 30-second bundling execution window
query = analyzed_traffic.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='20 seconds') \
    .start()

query.awaitTermination()