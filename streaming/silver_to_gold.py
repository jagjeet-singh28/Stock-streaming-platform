from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    min,
    max,
    avg,
    sum,
    count,
    first,
    last,
    round
)

spark = (
    SparkSession.builder
    .appName("StockSilverToGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 1. Read Silver as streaming DataFrame
# --------------------------------------------------

silver_stream = (
    spark.readStream
    .format("parquet")
    .schema("""
        event_id STRING,
        symbol STRING,
        exchange STRING,
        price DOUBLE,
        volume LONG,
        event_time TIMESTAMP,
        source STRING
    """)
    .load("data/silver/stock_ticks")
)

# --------------------------------------------------
# 2. Create 1-minute market metrics
# --------------------------------------------------

metrics = (
    silver_stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol"),
        col("exchange")
    )
    .agg(
        first("price").alias("open_price"),
        max("price").alias("high_price"),
        min("price").alias("low_price"),
        last("price").alias("close_price"),
        avg("price").alias("avg_price"),
        sum("volume").alias("total_volume"),
        count("*").alias("tick_count")
    )
)

# --------------------------------------------------
# 3. Calculate price movement
# --------------------------------------------------

gold_metrics = (
    metrics
    .withColumn(
        "price_change",
        round(col("close_price") - col("open_price"), 4)
    )
    .withColumn(
        "price_change_pct",
        round(
            (
                (col("close_price") - col("open_price"))
                / col("open_price")
            ) * 100,
            4
        )
    )
    .select(
        col("symbol"),
        col("exchange"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("open_price"),
        col("high_price"),
        col("low_price"),
        col("close_price"),
        round(col("avg_price"), 4).alias("avg_price"),
        col("price_change"),
        col("price_change_pct"),
        col("total_volume"),
        col("tick_count")
    )
)

# --------------------------------------------------
# 4. Write Gold stock_metrics
# --------------------------------------------------

query = (
    gold_metrics
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/gold/stock_metrics")
    .option("checkpointLocation", "data/checkpoints/gold_metrics")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()