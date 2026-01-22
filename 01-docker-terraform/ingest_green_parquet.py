#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine


@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='green_taxi_trips', help='Target table name')
@click.option('--file', default='green_tripdata_2025-11.parquet', help='Input Parquet file path')
def run(user, password, host, port, db, table, file):
    """Ingest Green Taxi Parquet data into PostgreSQL database."""
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read parquet
    df = pd.read_parquet(file)

    # Ensure datetime columns are parsed (green uses lpep_* names)
    for col in ['lpep_pickup_datetime', 'lpep_dropoff_datetime']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Create/replace table schema, then append data
    df.head(0).to_sql(name=table, con=engine, if_exists='replace', index=False)
    df.to_sql(name=table, con=engine, if_exists='append', index=False)

    print(f'Inserted {len(df)} rows into {table}')


if __name__ == '__main__':
    run()
