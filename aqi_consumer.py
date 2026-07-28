from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Initialize Spark Session with the Kafka SQL Connector package
spark = SparkSession.builder \
    .appName("SmartCity-AQI-Stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Suppress overly verbose info logs so your console remains clean
spark.sparkContext.setLogLevel("WARN")

# 2. Match your Python producer's JSON structure perfectly
aqi_schema = StructType([
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
    StructField("timestamp", StringType(), True)
])

# 3. Establish a streaming connection to your active Kafka topic
raw_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "aqi-data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Convert the binary Kafka value into a readable string and apply the schema
parsed_stream = raw_kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), aqi_schema).alias("data")) \
    .select("data.*")

# 5. Real-Time Analytics Transformation (Business Rule Layer)
# Your live baseline is high today (~232), so let's adjust safety brackets accordingly
analyzed_stream = parsed_stream.withColumn(
    "aqi_status",
    when(col("aqi") > 300, "Hazardous")
    .when(col("aqi") > 200, "Severe")
    .when(col("aqi") > 100, "Unhealthy")
    .otherwise("Acceptable")
).withColumn(
    "is_alert",
    when(col("aqi") > 200, True).otherwise(False)
)

# 6. Set up a real-time console sink to view the transformed output
query = analyzed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()