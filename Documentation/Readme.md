# Real-Time Stock Streaming Platform

A local, end-to-end real-time stock market streaming platform built using **Python, Apache Kafka, PySpark Structured Streaming, Parquet, and Streamlit**.

The project ingests live stock tick data, processes it through a **Medallion Architecture (Bronze → Silver → Gold)**, performs market analytics and anomaly detection, and exposes the results through an interactive dashboard.

---

## 1. Project Overview

This project demonstrates how a real-time data engineering pipeline can ingest, process, clean, aggregate, analyze, and visualize continuously arriving stock-market data.

### Use Case

The platform monitors selected Indian stocks and processes their market ticks in near real time.

The system answers questions such as:

* What is the latest stock price?
* What is the price movement within a window?
* How much trading volume has occurred?
* How active is a stock based on tick count?
* Are there unusual price or volume movements?
* How can processed streaming data be exposed to an end user?

---

# 2. Architecture

```text
                         ┌──────────────────────┐
                         │   Stock Market API   │
                         │       yfinance       │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │   Python Producer    │
                         │                      │
                         │  Fetch + Serialize   │
                         │       JSON           │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    Apache Kafka      │
                         │                      │
                         │    stock_ticks       │
                         └──────────┬───────────┘
                                    │
                                    ▼
                  ┌──────────────────────────────────┐
                  │       PySpark Structured         │
                  │            Streaming             │
                  └────────────────┬─────────────────┘
                                   │
                                   ▼
                         ┌──────────────────────┐
                         │       BRONZE         │
                         │                      │
                         │ Raw streaming events │
                         │      Parquet         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │       SILVER         │
                         │                      │
                         │ Validation           │
                         │ Deduplication        │
                         │ Watermarking         │
                         │ Data quality         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                     ┌─────────────────────────────┐
                     │            GOLD             │
                     │                             │
                     │  ┌───────────────────────┐  │
                     │  │    stock_metrics      │  │
                     │  │                       │  │
                     │  │ Market analytics      │  │
                     │  └───────────────────────┘  │
                     │                             │
                     │  ┌───────────────────────┐  │
                     │  │   stock_anomalies     │  │
                     │  │                       │  │
                     │  │ Unusual events        │  │
                     │  └───────────────────────┘  │
                     └──────────────┬──────────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │     Streamlit        │
                         │      Dashboard       │
                         └──────────────────────┘
```

---

# 3. Technology Stack

| Component         | Technology                   |
| ----------------- | ---------------------------- |
| Programming       | Python                       |
| Messaging         | Apache Kafka                 |
| Stream Processing | PySpark Structured Streaming |
| Storage           | Parquet                      |
| Visualization     | Streamlit                    |
| Containerization  | Docker                       |
| Data Source       | yfinance                     |
| Runtime           | Local Mac environment        |
| Java              | OpenJDK 17                   |
| Spark             | PySpark 4.x                  |

---

# 4. Project Structure

```text
stock-streaming-platform/
│
├── producer/
│   └── kafka_producer.py
│
├── streaming/
│   ├── kafka_to_bronze.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│
├── dashboard/
│   └── dashboard.py
│
├── gold_analysis.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── checkpoints/
│
├── README.md
├── requirements.txt
└── .gitignore
```

> Generated data and checkpoint directories should be excluded from Git using `.gitignore`.

---

# 5. Data Flow

## Step 1 — Data Ingestion

The Python producer retrieves stock-market data from the market-data source.

The initial stock universe includes:

```text
INFY
RELIANCE
TCS
```

Each event is converted into a JSON message.

Example:

```json
{
  "event_id": "unique-event-id",
  "symbol": "INFY",
  "exchange": "NSE",
  "price": 1500.50,
  "volume": 1200,
  "event_time": "2026-09-23T10:30:00",
  "source": "yfinance"
}
```

---

# 6. Kafka Layer

Kafka acts as the real-time event streaming layer.

### Topic

```text
stock_ticks
```

### Configuration

```text
Partitions: 3
Replication Factor: 1
Broker: localhost:9092
```

The producer publishes stock events to Kafka.

PySpark consumes these events continuously.

---

# 7. Bronze Layer

The Bronze layer stores the incoming Kafka events with minimal transformation.

### Responsibilities

* Consume Kafka messages
* Parse JSON
* Convert messages into structured Spark records
* Persist raw streaming data
* Maintain streaming checkpoints

### Storage

```text
data/bronze/stock_ticks
```

### Checkpoint

```text
data/checkpoints/bronze
```

The Bronze layer provides a persistent representation of the incoming streaming data.

---

# 8. Silver Layer

The Silver layer performs data cleansing and quality processing.

### Processing performed

#### Null validation

Required fields such as:

```text
event_id
symbol
event_time
```

are validated.

#### Price validation

Only records with:

```text
price > 0
```

are accepted.

#### Volume validation

Only records with:

```text
volume >= 0
```

are accepted.

#### Watermarking

A watermark is used to handle late-arriving events.

Current configuration:

```text
10 minutes
```

#### Deduplication

Duplicate events are removed using the relevant stock/time combination.

### Storage

```text
data/silver/stock_ticks
```

---

# 9. Gold Layer

The Gold layer contains business-oriented datasets designed for analytics and consumption.

Two Gold datasets were created.

---

## 9.1 stock_metrics

`stock_metrics` contains normal market analytics.

The current schema includes:

```text
symbol
window_start
window_end
open_price
high_price
low_price
close_price
avg_price
total_volume
tick_count
```

### Meaning

| Column         | Description                 |
| -------------- | --------------------------- |
| `symbol`       | Stock symbol                |
| `window_start` | Start of aggregation window |
| `window_end`   | End of aggregation window   |
| `open_price`   | Opening price in the window |
| `high_price`   | Highest price               |
| `low_price`    | Lowest price                |
| `close_price`  | Closing price               |
| `avg_price`    | Average price               |
| `total_volume` | Total traded volume         |
| `tick_count`   | Number of events/ticks      |

---

## 9.2 stock_anomalies

`stock_anomalies` contains only unusual market events detected by the Gold processing logic.

This separates normal market activity from events requiring additional attention.

The dashboard can therefore consume:

```text
stock_metrics
      ↓
Normal market monitoring

stock_anomalies
      ↓
Alerting / anomaly monitoring
```

---

# 10. Gold Analysis

The `gold_analysis.py` script reads the Gold Parquet datasets as normal Spark DataFrames.

Example:

```python
metrics_df = spark.read.parquet(
    "data/gold/stock_metrics"
)

anomalies_df = spark.read.parquet(
    "data/gold/stock_anomalies"
)
```

This enables batch-style analysis on top of the streaming output.

Examples include:

* Stock-level summaries
* Trading volume analysis
* Price analysis
* Anomaly counts
* Window-level analysis

---

# 11. Dashboard

The Streamlit dashboard consumes the Gold datasets.

Current dashboard functionality includes:

### Market KPIs

* Stocks tracked
* Number of anomalies
* Total volume

### Stock Metrics

Displays:

```text
Symbol
Window Start
Window End
Open
High
Low
Close
Average Price
Volume
Tick Count
Change %
```

### Charts

* Closing price over time
* Trading volume over time

### Anomaly Monitoring

Displays detected anomalies from:

```text
stock_anomalies
```

If no anomalies exist:

```text
No anomalies detected in the current Gold data.
```

---

# 12. Running the Project

## Prerequisites

Install:

* Docker
* Python 3.9+
* Java 17
* Apache Spark
* PySpark
* Streamlit

---

## Start Kafka

Verify Docker:

```bash
docker ps
```

Start the Kafka container if required:

```bash
docker start stock-kafka
```

---

## Start the Producer

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Run:

```bash
python producer/kafka_producer.py
```

The producer continuously publishes stock events to:

```text
stock_ticks
```

---

## Start Bronze

Run:

```bash
spark-submit streaming/kafka_to_bronze.py
```

---

## Start Silver

Run:

```bash
spark-submit streaming/bronze_to_silver.py
```

---

## Start Gold

Run:

```bash
spark-submit streaming/silver_to_gold.py
```

---

## Start Dashboard

Run:

```bash
streamlit run dashboard/dashboard.py
```

The dashboard will be available locally through the Streamlit URL displayed in the terminal.

---

# 13. Streaming Execution Model

The project uses multiple continuously running processes.

```text
Terminal 1
Python Producer
       ↓
Kafka

Terminal 2
Kafka → Bronze

Terminal 3
Bronze → Silver

Terminal 4
Silver → Gold

Terminal 5
Streamlit Dashboard
```

Once all processes are running:

```text
New stock tick
      ↓
Kafka
      ↓
Bronze
      ↓
Silver
      ↓
Gold
      ↓
Dashboard
```

---

# 14. Key Streaming Concepts Demonstrated

This project demonstrates several concepts relevant to real-world data engineering.

### Apache Kafka

Used as the event-streaming and decoupling layer between ingestion and processing.

### Spark Structured Streaming

Used for continuous stream processing.

### Checkpointing

Used to maintain streaming progress and support recovery.

### Watermarking

Used to manage late-arriving events.

### Deduplication

Used to prevent duplicate events from propagating downstream.

### Windowed Aggregations

Used to convert individual stock ticks into meaningful market metrics.

### Medallion Architecture

```text
Bronze → Raw
Silver → Cleaned
Gold → Business-ready
```

---

# 15. Why This Architecture?

The architecture separates different responsibilities.

### Bronze

Preserve incoming data.

### Silver

Clean and validate the data.

### Gold

Create analytics-ready datasets.

### Dashboard

Expose business information to users.

This separation makes the system easier to maintain, troubleshoot, and extend.

---

# 16. Failure Recovery

Streaming checkpoints are maintained separately from the data.

Example:

```text
data/checkpoints/bronze
```

The checkpoint allows Spark to maintain processing state across batches and recover from interruptions.

This is important in streaming systems because the application must track what has already been processed.

---

# 17. Future Improvements

Potential next steps include:

### Real-time dashboard refresh

Automatically refresh the Streamlit dashboard as new Gold records arrive.

### More advanced anomaly detection

Examples:

* Volume spikes
* Sudden price movements
* Z-score based anomalies
* Moving-average deviations
* Volatility detection

### Alerting

Send anomaly notifications through:

* Telegram
* Email
* Slack

### More stocks

Expand the stock universe beyond:

```text
TCS
INFY
RELIANCE
```

### Cloud deployment

The local architecture can be adapted to a cloud platform such as Azure.

A production-oriented version could use:

```text
Market Data
     ↓
Event Streaming
     ↓
Azure / Kafka
     ↓
Databricks
     ↓
Delta Lake
     ↓
Gold Tables
     ↓
Power BI / Dashboard
```

---