from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    max,
    min,
    sum,
    count,
    round
)

# =========================
# CREATE SPARK SESSION
# =========================

spark = (
    SparkSession.builder
    .appName("GoldAnalysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================
# READ GOLD DATA
# =========================

metrics_df = spark.read.parquet(
    "data/gold/stock_metrics"
)

anomalies_df = spark.read.parquet(
    "data/gold/stock_anomalies"
)


# =========================
# INSPECT DATA
# =========================

print("\n===== STOCK METRICS SCHEMA =====")
metrics_df.printSchema()

print("\n===== STOCK ANOMALIES SCHEMA =====")
anomalies_df.printSchema()


print("\n===== STOCK METRICS =====")
metrics_df.show(20, truncate=False)

print("\n===== STOCK ANOMALIES =====")
anomalies_df.show(20, truncate=False)


# =========================
# STOCK SUMMARY
# =========================

print("\n===== STOCK SUMMARY =====")

(
    metrics_df
    .groupBy("symbol")
    .agg(
        round(avg("avg_price"), 2).alias("avg_price"),
        round(min("low_price"), 2).alias("lowest_price"),
        round(max("high_price"), 2).alias("highest_price"),
        sum("total_volume").alias("total_volume")
    )
    .orderBy(col("total_volume").desc())
    .show(truncate=False)
)


# =========================
# HIGHEST VOLUME
# =========================

print("\n===== HIGHEST VOLUME =====")

(
    metrics_df
    .select(
        "symbol",
        "window_start",
        "window_end",
        "total_volume"
    )
    .orderBy(
        col("total_volume").desc()
    )
    .show(10, truncate=False)
)


# =========================
# LARGEST PRICE MOVEMENTS
# =========================

#print("\n===== LARGEST PRICE MOVEMENTS =====")

#(
#    metrics_df
#    .select(
#        "symbol",
#        "window_start",
#        "window_end",
#        "price_change"
#    )
#    .orderBy(
#        col("price_change").desc()
#    )
#    .show(10, truncate=False)
#)


# =========================
# ANOMALY SUMMARY
# =========================

print("\n===== ANOMALY SUMMARY =====")

(
    anomalies_df
    .groupBy("symbol")
    .agg(
        count("*").alias("anomaly_count")
    )
    .orderBy(
        col("anomaly_count").desc()
    )
    .show(truncate=False)
)

print("\n===== RECORD COUNTS =====")

print(
    "Metrics records:",
    metrics_df.count()
)

print(
    "Anomaly records:",
    anomalies_df.count()
)
# =========================
# STOP SPARK
# =========================

spark.stop()