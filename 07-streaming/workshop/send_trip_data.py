import json
from time import time

import pandas as pd
from kafka import KafkaProducer


TOPIC_NAME = "green-trips"
BOOTSTRAP_SERVERS = ["localhost:9092"]
DATA_FILE = "green_tripdata_2025-10.parquet"

COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def normalize_record(record):
    normalized = {}
    for key, value in record.items():
        if pd.isna(value):
            normalized[key] = None
        elif "datetime" in key:
            normalized[key] = str(value)
        else:
            normalized[key] = value
    return normalized


def main():
    df = pd.read_parquet(DATA_FILE, columns=COLUMNS)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
    )

    t0 = time()

    for record in df.to_dict(orient="records"):
        producer.send(TOPIC_NAME, value=normalize_record(record))

    producer.flush()

    t1 = time()
    print(f"took {(t1 - t0):.2f} seconds")


if __name__ == "__main__":
    main()