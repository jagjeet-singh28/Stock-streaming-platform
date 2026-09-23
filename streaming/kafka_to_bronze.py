from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType
)

spark = (
    SparkSession.builder
    .appName("StockKafkaToBronze")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("source", StringType(), True)
])

# Read from Kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock_ticks")
    .option("startingOffsets", "latest")
    .load()
)

# Kafka value -> JSON string
json_stream = raw_stream.select(
    col("value").cast("string").alias("json")
)

# JSON -> structured columns
stock_stream = (
    json_stream
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

# Write Bronze layer
query = (
    stock_stream.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/bronze/stock_ticks")
    .option("checkpointLocation", "data/checkpoints/bronze")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()    