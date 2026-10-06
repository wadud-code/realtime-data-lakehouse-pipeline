
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Initialize Spark Session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaPySparkSilverStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for raw JSON messages coming from Kafka
order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("item", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_timestamp", StringType(), True)
])

# 1. Read continuous stream from Kafka Topic
raw_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "order_events") \
    .option("startingOffsets", "latest") \
    .load()

# 2. Parse raw bytes into structured DataFrame
parsed_stream = raw_kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("processed_at", current_timestamp())

# 3. Write structured stream to local S3 Silver bucket in Parquet format
query = parsed_stream.writeStream \
    .format("parquet") \
    .option("path", "s3a://tf-data-lake-silver/silver/orders/") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/orders/") \
    .outputMode("append") \
    .start()

print("PySpark Structured Streaming query active. Listening to Kafka...")
query.awaitTermination()
