from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("StockBronzeToSilver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read Bronze as a streaming DataFrame
bronze_stream = (
    spark.readStream
    .schema("""
        event_id STRING,
        symbol STRING,
        exchange STRING,
        price DOUBLE,
        volume LONG,
        event_time TIMESTAMP,
        source STRING
    """)
    .parquet("data/bronze/stock_ticks")
)

# Data quality + event-time handling + deduplication
silver_stream = (
    bronze_stream
    .filter(col("event_id").isNotNull())
    .filter(col("symbol").isNotNull())
    .filter(col("event_time").isNotNull())
    .filter(col("price") > 0)
    .filter(col("volume") >= 0)
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["symbol", "event_time"])
)

# Write Silver
query = (
    silver_stream.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/silver/stock_ticks")
    .option("checkpointLocation", "data/checkpoints/silver")
    .trigger(processingTime="10 seconds")
    .start()
)
print("Starting Silver streaming query...")

query = (
    silver_stream.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/silver/stock_ticks")
    .option("checkpointLocation", "data/checkpoints/silver")
    .trigger(processingTime="10 seconds")
    .start()
)

print("Silver streaming query started!")
query.awaitTermination()