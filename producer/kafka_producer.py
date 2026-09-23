import json
import time
import uuid

import yfinance as yf
from kafka import KafkaProducer


# Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

stocks = [
    "RELIANCE.NS",
    "TCS.NS",
    "INFY.NS"
]


def get_latest_price(symbol):
    ticker = yf.Ticker(symbol)

    data = ticker.history(
        period="1d",
        interval="1m"
    )

    if data.empty:
        return None

    latest = data.iloc[-1]

    return {
        "event_id": str(uuid.uuid4()),
        "symbol": symbol.replace(".NS", ""),
        "exchange": "NSE",
        "price": float(latest["Close"]),
        "volume": int(latest["Volume"]),
        "event_time": data.index[-1].isoformat(),
        "source": "yfinance"
    }


while True:

    for symbol in stocks:

        try:
            event = get_latest_price(symbol)

            if event:
                producer.send(
                    "stock_ticks",
                    key=event["symbol"],
                    value=event
                )

                print(f"Sent: {event}")

        except Exception as e:
            print(f"Error for {symbol}: {e}")

    producer.flush()

    time.sleep(60)