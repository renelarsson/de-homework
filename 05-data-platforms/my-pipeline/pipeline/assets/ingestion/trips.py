"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default 

depends_on:
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: append
@bruin"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable

import pandas as pd
import requests


@dataclass(frozen=True)
class RunWindow:
  start_date: date
  end_date: date


def generate_months_to_ingest(window: RunWindow) -> list[tuple[int, int]]:
  """Return (year, month) pairs where the month start is within [start_date, end_date)."""
  current = window.start_date.replace(day=1)
  end = window.end_date
  months: list[tuple[int, int]] = []
  while current < end:
    months.append((current.year, current.month))
    if current.month == 12:
      current = current.replace(year=current.year + 1, month=1)
    else:
      current = current.replace(month=current.month + 1)
  return months


def build_parquet_url(taxi_type: str, year: int, month: int) -> str:
  """NYC TLC parquet files hosted on CloudFront."""
  mm = f"{month:02d}"
  return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{mm}.parquet"


def _snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
  df = df.copy()
  df.columns = [
    str(c)
    .strip()
    .replace(" ", "_")
    .replace("-", "_")
    .replace("__", "_")
    .lower()
    for c in df.columns
  ]
  return df


def _normalize_timestamps(df: pd.DataFrame) -> pd.DataFrame:
  df = df.copy()
  rename_map = {
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
  }
  for src, dst in rename_map.items():
    if src in df.columns and dst not in df.columns:
      df = df.rename(columns={src: dst})
  for col in ("pickup_datetime", "dropoff_datetime"):
    if col in df.columns:
      df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
  return df


def _normalize_common_columns(df: pd.DataFrame) -> pd.DataFrame:
  df = df.copy()

  rename_map = {
    # TLC parquet uses PULocationID / DOLocationID -> snake_case becomes pulocationid/dolocationid
    "pulocationid": "pickup_location_id",
    "dolocationid": "dropoff_location_id",
    # Some variants
    "pu_location_id": "pickup_location_id",
    "do_location_id": "dropoff_location_id",
  }

  for src, dst in rename_map.items():
    if src in df.columns and dst not in df.columns:
      df = df.rename(columns={src: dst})

  return df


def fetch_trip_data(url: str, timeout_s: int = 120) -> pd.DataFrame | None:
  """Download a parquet file and return a DataFrame.

  Returns None when the remote object doesn't exist (commonly surfaced as 403/404
  by the TLC CloudFront distribution for future months).
  """
  with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
    with requests.get(url, stream=True, timeout=timeout_s) as resp:
      if resp.status_code in (403, 404):
        return None
      resp.raise_for_status()
      for chunk in resp.iter_content(chunk_size=1024 * 1024):
        if chunk:
          tmp.write(chunk)
    tmp.flush()
    df = pd.read_parquet(tmp.name)
  return df


def _parse_window_from_env() -> RunWindow:
  start_s = os.environ.get("BRUIN_START_DATE")
  end_s = os.environ.get("BRUIN_END_DATE")
  if not start_s or not end_s:
    raise RuntimeError("Expected BRUIN_START_DATE and BRUIN_END_DATE to be set")

  # Bruin/VS Code integrations may pass timestamp-like strings; we only need the date.
  # Examples: "2026-02-23" or "2026-02-23T00:00:00.000Z".
  start_s = start_s[:10]
  end_s = end_s[:10]
  return RunWindow(
    start_date=date.fromisoformat(start_s),
    end_date=date.fromisoformat(end_s),
  )


def _read_taxi_types_from_vars(default: list[str] | None = None) -> list[str]:
  raw = os.environ.get("BRUIN_VARS")
  if not raw:
    return default or ["yellow"]
  try:
    parsed = json.loads(raw)
  except json.JSONDecodeError:
    return default or ["yellow"]
  taxi_types = parsed.get("taxi_types")
  if isinstance(taxi_types, list) and all(isinstance(x, str) for x in taxi_types):
    return taxi_types
  return default or ["yellow"]


def _iter_urls(taxi_types: Iterable[str], months: list[tuple[int, int]]) -> Iterable[tuple[str, str]]:
  for taxi_type in taxi_types:
    for year, month in months:
      yield taxi_type, build_parquet_url(taxi_type, year, month)


def materialize() -> pd.DataFrame:
  """Bruin entrypoint: returns a DataFrame that Bruin materializes into DuckDB."""
  window = _parse_window_from_env()
  taxi_types = _read_taxi_types_from_vars(default=["yellow"])
  months = generate_months_to_ingest(window)

  extracted_at = datetime.now(tz=timezone.utc).isoformat()
  frames: list[pd.DataFrame] = []
  for taxi_type, url in _iter_urls(taxi_types, months):
    df = fetch_trip_data(url)
    if df is None:
      print(f"Skipping missing parquet (likely not published yet): {url}")
      continue
    df = _snake_case_columns(df)
    df = _normalize_timestamps(df)
    df = _normalize_common_columns(df)
    df["taxi_type"] = taxi_type
    df["source_url"] = url
    df["extracted_at"] = extracted_at
    frames.append(df)

  if not frames:
    return pd.DataFrame(
      columns=[
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "fare_amount",
        "total_amount",
        "taxi_type",
        "payment_type",
        "source_url",
        "extracted_at",
      ]
    )

  out = pd.concat(frames, ignore_index=True)
  if out.empty:
    return out
  if "pickup_datetime" not in out.columns:
    raise RuntimeError("Expected a 'pickup_datetime' column after normalization")
  return out
