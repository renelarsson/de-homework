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
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: payment_type_name
    type: VARCHAR
    description: Payment type label.
    primary_key: true
  - name: pickup_date
    type: DATE
    description: Pickup date.
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips.
    checks:
      - name: non_negative
  - name: total_amount_sum
    type: DOUBLE
    description: Sum of total_amount.
    # Note: TLC data may include negative totals (e.g., adjustments/refunds),
    # so we intentionally do not enforce non_negative here.

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  COALESCE(payment_type_name, 'unknown') AS payment_type_name,
  CAST(date_trunc('day', pickup_datetime) AS DATE) AS pickup_date,
  COUNT(*) AS trip_count,
  SUM(COALESCE(total_amount, 0)) AS total_amount_sum
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2
