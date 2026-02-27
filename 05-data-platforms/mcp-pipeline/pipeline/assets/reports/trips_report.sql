/* @bruin

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: date

columns:
  - name: report_date
    type: date
    description: Date of the report
    primary_key: true
  - name: taxi_type
    type: string
    description: Type of taxi (e.g., yellow, green)
    primary_key: true
  - name: payment_type_name
    type: string
    description: Name of the payment type
    primary_key: true
  - name: total_trips
    type: bigint
    description: Total number of trips
    checks:
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: Total number of passengers
    checks:
      - name: non_negative
  - name: total_distance
    type: double
    description: Total distance covered by trips
    checks:
      - name: non_negative

@bruin */

SELECT
    DATE(pickup_datetime) AS report_date,
    taxi_type,
    payment_type_name,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY report_date, taxi_type, payment_type_name;
