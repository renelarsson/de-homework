"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: trip_id
    type: integer
    description: Unique identifier for each trip
    primary_key: true
  - name: pickup_datetime
    type: timestamp
    description: Pickup date and time
  - name: dropoff_datetime
    type: timestamp
    description: Dropoff date and time
  - name: passenger_count
    type: integer
    description: Number of passengers
  - name: trip_distance
    type: float
    description: Distance of the trip in miles
  - name: taxi_type
    type: string
    description: Type of taxi (e.g., yellow, green)
  - name: payment_type_id
    type: integer
    description: Numeric payment type code from TLC data

@bruin"""

import json
import os
from typing import List

import pandas as pd
from dateutil import parser as date_parser


BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "{taxi_type}_tripdata_{year}-{month:02d}.parquet"
)


def _month_periods(start_date: str, end_date: str) -> List[pd.Period]:
    """Return all month periods overlapping the [start_date, end_date) interval."""
    start = date_parser.isoparse(start_date)
    end = date_parser.isoparse(end_date)

    # end is exclusive, subtract a day so we include the last month that has data
    end_inclusive = end - pd.Timedelta(days=1)
    if end_inclusive < start:
        return []

    return list(pd.period_range(start=start, end=end_inclusive, freq="M"))


def _load_month(taxi_type: str, year: int, month: int) -> pd.DataFrame:
    url = BASE_URL.format(taxi_type=taxi_type, year=year, month=month)
    df = pd.read_parquet(url)

    # Normalize pickup/dropoff + payment columns across yellow/green
    if "tpep_pickup_datetime" in df.columns:
        df = df.rename(
            columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "payment_type": "payment_type_id",
            }
        )
    elif "lpep_pickup_datetime" in df.columns:
        df = df.rename(
            columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
                "payment_type": "payment_type_id",
            }
        )

    df["taxi_type"] = taxi_type
    return df


def materialize() -> pd.DataFrame:
    """Fetch NYC TLC parquet data for the requested window and taxi types.

    Uses Bruin-provided environment variables:
    - BRUIN_START_DATE / BRUIN_END_DATE for the time window
    - BRUIN_VARS (JSON) for the `taxi_types` variable
    """

    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    raw_vars = os.environ.get("BRUIN_VARS", "{}") or "{}"
    vars_dict = json.loads(raw_vars)
    taxi_types = vars_dict.get("taxi_types", ["yellow"])

    periods = _month_periods(start_date, end_date)
    frames: List[pd.DataFrame] = []

    for taxi_type in taxi_types:
        for period in periods:
            df_month = _load_month(taxi_type, period.year, period.month)
            frames.append(df_month)

    if not frames:
        # Return an empty frame with the expected schema
        return pd.DataFrame(
            columns=[
                "trip_id",
                "pickup_datetime",
                "dropoff_datetime",
                "passenger_count",
                "trip_distance",
                "taxi_type",
                "payment_type_id",
            ]
        )

    df = pd.concat(frames, ignore_index=True)

    # Ensure pickup_datetime is a proper timestamp for filtering
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])

    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    mask = (df["pickup_datetime"] >= start_ts) & (df["pickup_datetime"] < end_ts)
    df = df.loc[mask].copy()

    # Keep only the columns we need and add a synthetic trip_id
    expected_cols = [
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "taxi_type",
        "payment_type_id",
    ]

    df = df[expected_cols]
    df.insert(0, "trip_id", df.index.astype("int64"))

    return df


