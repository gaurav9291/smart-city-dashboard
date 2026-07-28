from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SmartCity-Weather-Stream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema matching our producer's payload keys
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

# Connect to Kafka topic
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), weather_schema).alias("data")) \
    .select("data.*")

# Add transformation logic (e.g., flagging extreme heat or heavy rainfall anomalies)
analyzed_weather = parsed_stream.withColumn(
    "weather_condition",
    when(col("rainfall_mm") > 5.0, "Heavy Rain")
    .when(col("temperature_c") > 38.0, "Extreme Heat")
    .otherwise("Normal")
)

# Output stream directly to Console
# Change your existing query block to this:
query = analyzed_weather.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='20 seconds') \
    .start()
    
    
query.awaitTermination()