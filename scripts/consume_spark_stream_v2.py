
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Initialize Spark Session with explicit packages
spark = SparkSession.builder \
    .appName("KafkaPySparkSilverStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("item", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_timestamp", StringType(), True)
])

# Read from Kafka Topic
raw_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "order_events") \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = raw_kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("processed_at", current_timestamp())

# Output directly to Terminal Console
query = parsed_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

print("--- PySpark Active: Streaming from Kafka to Console ---")
query.awaitTermination()
