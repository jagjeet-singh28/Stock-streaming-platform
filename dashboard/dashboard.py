
import streamlit as st
import pandas as pd
import time
from pyspark.sql import SparkSession


# ============================================================
# STREAMLIT CONFIG
# ============================================================

st.set_page_config(
    page_title="Stock Streaming Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Stock Streaming Dashboard")
st.caption(
    "Kafka → Bronze → Silver → Gold → Dashboard"
)


# ============================================================
# PATHS
# ============================================================

METRICS_PATH = "data/gold/stock_metrics"
ANOMALIES_PATH = "data/gold/stock_anomalies"


# ============================================================
# SPARK SESSION
# ============================================================

@st.cache_resource
def get_spark():

    return (
        SparkSession.builder
        .appName("StockStreamingDashboard")
        .master("local[*]")
        .getOrCreate()
    )


spark = get_spark()

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# LOAD GOLD METRICS
# ============================================================

@st.cache_data(ttl=10)
def load_metrics():

    try:

        df = (
            spark.read
            .parquet(METRICS_PATH)
        )

        return df.toPandas()

    except Exception:

        return pd.DataFrame()


# ============================================================
# LOAD GOLD ANOMALIES
# ============================================================

@st.cache_data(ttl=10)
def load_anomalies():

    try:

        df = (
            spark.read
            .parquet(ANOMALIES_PATH)
        )

        return df.toPandas()

    except Exception:

        return pd.DataFrame()


metrics_df = load_metrics()
anomalies_df = load_anomalies()


# ============================================================
# EMPTY STATE
# ============================================================

if metrics_df.empty:

    st.warning(
        "No Gold stock metrics available yet."
    )

    st.info(
        "Make sure Kafka → Bronze → Silver → Gold is running "
        "and data is being written to data/gold/stock_metrics."
    )

    st.stop()


# ============================================================
# DATA CLEANUP
# ============================================================

metrics_df["window_start"] = pd.to_datetime(
    metrics_df["window_start"]
)

metrics_df["window_end"] = pd.to_datetime(
    metrics_df["window_end"]
)

metrics_df = metrics_df.sort_values(
    "window_start"
)


if not anomalies_df.empty:

    anomalies_df["window_start"] = pd.to_datetime(
        anomalies_df["window_start"]
    )

    anomalies_df["window_end"] = pd.to_datetime(
        anomalies_df["window_end"]
    )

    anomalies_df = anomalies_df.sort_values(
        "window_start",
        ascending=False
    )


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header("Dashboard Controls")


# ------------------------------------------------------------
# Symbol filter
# ------------------------------------------------------------

symbols = sorted(
    metrics_df["symbol"]
    .dropna()
    .unique()
)

selected_symbol = st.sidebar.selectbox(
    "Select Stock",
    ["All"] + symbols
)


# ------------------------------------------------------------
# Number of rows
# ------------------------------------------------------------

row_limit = st.sidebar.slider(
    "Rows to display",
    min_value=10,
    max_value=100,
    value=30,
    step=10
)


# ------------------------------------------------------------
# Auto refresh
# ------------------------------------------------------------

auto_refresh = st.sidebar.checkbox(
    "Auto refresh",
    value=True
)


# ============================================================
# APPLY FILTER
# ============================================================

if selected_symbol == "All":

    filtered_metrics = metrics_df.copy()

else:

    filtered_metrics = metrics_df[
        metrics_df["symbol"] == selected_symbol
    ].copy()


if filtered_metrics.empty:

    st.warning(
        f"No metrics available for {selected_symbol}."
    )

    st.stop()


# ============================================================
# LATEST RECORD
# ============================================================

latest = (
    filtered_metrics
    .sort_values("window_start")
    .iloc[-1]
)


# ============================================================
# TOP METRICS
# ============================================================

st.subheader("Market Overview")


col1, col2, col3, col4, col5 = st.columns(5)


# ------------------------------------------------------------
# Latest price
# ------------------------------------------------------------

with col1:

    st.metric(
        "Latest Price",
        f"₹{latest['close_price']:,.2f}"
    )


# ------------------------------------------------------------
# Price change
# ------------------------------------------------------------

with col2:

    price_change = latest["price_change"]

    st.metric(
        "Price Change",
        f"₹{price_change:,.2f}"
    )


# ------------------------------------------------------------
# Price change %
# ------------------------------------------------------------

with col3:

    price_change_pct = latest["price_change_pct"]

    st.metric(
        "Price Change %",
        f"{price_change_pct:.2f}%"
    )


# ------------------------------------------------------------
# Volume
# ------------------------------------------------------------

with col4:

    st.metric(
        "Volume",
        f"{int(latest['total_volume']):,}"
    )


# ------------------------------------------------------------
# Tick count
# ------------------------------------------------------------

with col5:

    st.metric(
        "Ticks",
        int(latest["tick_count"])
    )


# ============================================================
# PRICE CHART
# ============================================================

st.subheader("Price Movement")


price_chart_df = (
    filtered_metrics
    .sort_values("window_start")
    .tail(row_limit)
    .set_index("window_start")
)


st.line_chart(
    price_chart_df["close_price"],
    use_container_width=True
)


# ============================================================
# PRICE CHANGE %
# ============================================================

st.subheader("Price Change %")


price_change_chart = (
    filtered_metrics
    .sort_values("window_start")
    .tail(row_limit)
    .set_index("window_start")
)


st.line_chart(
    price_change_chart["price_change_pct"],
    use_container_width=True
)


# ============================================================
# VOLUME CHART
# ============================================================

st.subheader("Trading Volume")


volume_chart_df = (
    filtered_metrics
    .sort_values("window_start")
    .tail(row_limit)
    .set_index("window_start")
)


st.bar_chart(
    volume_chart_df["total_volume"],
    use_container_width=True
)


# ============================================================
# OHLC DATA
# ============================================================

st.subheader("OHLC Market Data")


ohlc_columns = [
    "symbol",
    "exchange",
    "window_start",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "avg_price",
    "price_change",
    "price_change_pct",
    "total_volume",
    "tick_count"
]


available_ohlc_columns = [
    column
    for column in ohlc_columns
    if column in filtered_metrics.columns
]


ohlc_display = (
    filtered_metrics
    .sort_values("window_start", ascending=False)
    .head(row_limit)
    [available_ohlc_columns]
    .copy()
)


st.dataframe(
    ohlc_display,
    use_container_width=True,
    hide_index=True
)


# ============================================================
# ANOMALIES
# ============================================================

st.subheader("🚨 Stock Anomalies")


if anomalies_df.empty:

    st.success(
        "No anomalies detected."
    )

else:

    # Apply symbol filter
    if selected_symbol != "All":

        filtered_anomalies = anomalies_df[
            anomalies_df["symbol"] == selected_symbol
        ].copy()

    else:

        filtered_anomalies = anomalies_df.copy()


    if filtered_anomalies.empty:

        st.success(
            f"No anomalies detected for {selected_symbol}."
        )

    else:

        anomaly_columns = [
            "symbol",
            "exchange",
            "window_start",
            "window_end",
            "open_price",
            "close_price",
            "price_change",
            "price_change_pct",
            "total_volume",
            "avg_volume",
            "volume_ratio",
            "anomaly_type"
        ]


        available_anomaly_columns = [
            column
            for column in anomaly_columns
            if column in filtered_anomalies.columns
        ]


        anomaly_display = (
            filtered_anomalies
            .head(row_limit)
            [available_anomaly_columns]
            .copy()
        )


        st.dataframe(
            anomaly_display,
            use_container_width=True,
            hide_index=True
        )


# ============================================================
# ANOMALY SUMMARY
# ============================================================

st.subheader("Anomaly Summary")


if anomalies_df.empty:

    summary_col1, summary_col2, summary_col3 = st.columns(3)

    with summary_col1:

        st.metric(
            "Total Anomalies",
            0
        )

    with summary_col2:

        st.metric(
            "Price Anomalies",
            0
        )

    with summary_col3:

        st.metric(
            "Volume Anomalies",
            0
        )

else:

    summary_df = anomalies_df.copy()

    if selected_symbol != "All":

        summary_df = summary_df[
            summary_df["symbol"] == selected_symbol
        ]


    total_anomalies = len(summary_df)

    price_anomalies = len(
        summary_df[
            summary_df["anomaly_type"].isin(
                ["PRICE", "PRICE_AND_VOLUME"]
            )
        ]
    )

    volume_anomalies = len(
        summary_df[
            summary_df["anomaly_type"].isin(
                ["VOLUME", "PRICE_AND_VOLUME"]
            )
        ]
    )

    combined_anomalies = len(
        summary_df[
            summary_df["anomaly_type"] == "PRICE_AND_VOLUME"
        ]
    )


    summary_col1, summary_col2, summary_col3, summary_col4 = (
        st.columns(4)
    )


    with summary_col1:

        st.metric(
            "Total Anomalies",
            total_anomalies
        )


    with summary_col2:

        st.metric(
            "Price Anomalies",
            price_anomalies
        )


    with summary_col3:

        st.metric(
            "Volume Anomalies",
            volume_anomalies
        )


    with summary_col4:

        st.metric(
            "Price + Volume",
            combined_anomalies
        )


# ============================================================
# LATEST DATA TABLE
# ============================================================

st.subheader("Latest Market Records")


latest_columns = [
    "symbol",
    "exchange",
    "window_start",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "price_change",
    "price_change_pct",
    "total_volume",
    "tick_count"
]


available_latest_columns = [
    column
    for column in latest_columns
    if column in filtered_metrics.columns
]


latest_display = (
    filtered_metrics
    .sort_values(
        "window_start",
        ascending=False
    )
    .head(10)
    [available_latest_columns]
)


st.dataframe(
    latest_display,
    use_container_width=True,
    hide_index=True
)


# ============================================================
# LAST UPDATED
# ============================================================

latest_timestamp = (
    filtered_metrics["window_end"]
    .max()
)


st.caption(
    f"Latest Gold data: {latest_timestamp}"
)


# ============================================================
# AUTO REFRESH
# ============================================================

if auto_refresh:

    time.sleep(10)

    st.rerun()
