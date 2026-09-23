from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    abs,
    round,
    when,
    lit
)

spark = (
    SparkSession.builder
    .appName("StockGoldAnomalies")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# Paths
# --------------------------------------------------

METRICS_PATH = "data/gold/stock_metrics"
ANOMALIES_PATH = "data/gold/stock_anomalies"
CHECKPOINT_PATH = "data/checkpoints/gold_anomalies"

# --------------------------------------------------
# Read stock_metrics as a stream
# --------------------------------------------------

metrics_stream = (
    spark.readStream
    .format("parquet")
    .schema("""
        symbol STRING,
        exchange STRING,
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        close_price DOUBLE,
        avg_price DOUBLE,
        price_change DOUBLE,
        price_change_pct DOUBLE,
        total_volume LONG,
        tick_count LONG
    """)
    .load(METRICS_PATH)
)


# --------------------------------------------------
# Process every micro-batch
# --------------------------------------------------

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print(f"\n========== Processing batch {batch_id} ==========")

    # --------------------------------------------------
    # Read historical metrics
    # --------------------------------------------------

    historical_df = (
        spark.read
        .parquet(METRICS_PATH)
    )

    # --------------------------------------------------
    # Find historical average volume
    #
    # Exclude the current batch by using only records
    # whose window_start is earlier than the current batch.
    # --------------------------------------------------

    current_start = batch_df.agg(
        {"window_start": "min"}
    ).collect()[0][0]

    historical_df = historical_df.filter(
        col("window_start") < lit(current_start)
    )

    volume_baseline = (
        historical_df
        .groupBy("symbol")
        .agg(
            avg("total_volume").alias("avg_volume")
        )
    )

    # --------------------------------------------------
    # Join current metrics with historical baseline
    # --------------------------------------------------

    evaluated = (
        batch_df
        .join(
            volume_baseline,
            on="symbol",
            how="left"
        )
        .withColumn(
            "volume_ratio",
            when(
                col("avg_volume") > 0,
                col("total_volume") / col("avg_volume")
            ).otherwise(lit(0.0))
        )
    )

    # --------------------------------------------------
    # Detect anomalies
    # --------------------------------------------------

    anomalies = (
        evaluated
        .withColumn(
            "price_anomaly",
            abs(col("price_change_pct")) >= 1.0
        )
        .withColumn(
            "volume_anomaly",
            col("volume_ratio") >= 3.0
        )
        .withColumn(
            "anomaly_type",
            when(
                col("price_anomaly") & col("volume_anomaly"),
                "PRICE_AND_VOLUME"
            )
            .when(
                col("price_anomaly"),
                "PRICE"
            )
            .when(
                col("volume_anomaly"),
                "VOLUME"
            )
        )
        .filter(
            col("anomaly_type").isNotNull()
        )
    )

    # --------------------------------------------------
    # Final anomaly table
    # --------------------------------------------------

    final_anomalies = (
        anomalies
        .select(
            "symbol",
            "exchange",
            "window_start",
            "window_end",
            "open_price",
            "close_price",
            "price_change",
            "price_change_pct",
            "total_volume",
            round("avg_volume", 2).alias("avg_volume"),
            round("volume_ratio", 2).alias("volume_ratio"),
            "anomaly_type"
        )
    )

    # --------------------------------------------------
    # Show anomalies in terminal
    # --------------------------------------------------

    print("\nDetected anomalies:")

    final_anomalies.show(
        truncate=False
    )

    # --------------------------------------------------
    # Write anomalies
    # --------------------------------------------------

    if not final_anomalies.isEmpty():

        (
            final_anomalies
            .write
            .mode("append")
            .parquet(ANOMALIES_PATH)
        )

        print(
            f"Written {final_anomalies.count()} anomalies "
            f"to {ANOMALIES_PATH}"
        )

    else:

        print("No anomalies detected.")


# --------------------------------------------------
# Start streaming query
# --------------------------------------------------

query = (
    metrics_stream
    .writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()