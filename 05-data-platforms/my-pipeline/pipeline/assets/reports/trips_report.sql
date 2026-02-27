/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report

# Docs: https://getbruin.com/docs/bruin/assets/sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  incremental_key: trip_date
  time_granularity: date

columns:
  - name: trip_date
    type: DATE
    description: Pickup date.
    primary_key: true
  - name: taxi_type
    type: VARCHAR
    description: Taxi type (yellow/green).
    primary_key: true
  - name: payment_type
    type: VARCHAR
    description: Payment type label.
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips.
    checks:
      - name: non_negative
  - name: total_fare
    type: DOUBLE
    description: Sum of fare_amount.
  - name: avg_fare
    type: DOUBLE
    description: Average fare_amount.

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    COALESCE(payment_type_name, 'unknown') AS payment_type,
    COUNT(*) AS trip_count,
    SUM(COALESCE(fare_amount, 0)) AS total_fare,
    AVG(COALESCE(fare_amount, 0)) AS avg_fare
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3
