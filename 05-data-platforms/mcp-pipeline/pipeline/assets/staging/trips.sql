/* @bruin

name: staging.trips

type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

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
  - name: payment_type_name
    type: string
    description: Name of the payment type

@bruin */

WITH deduplicated_trips AS (
    SELECT
        t.trip_id,
        t.pickup_datetime,
        t.dropoff_datetime,
        t.passenger_count,
        t.trip_distance,
        t.taxi_type,
        p.payment_type_name,
        ROW_NUMBER() OVER (PARTITION BY t.trip_id ORDER BY t.pickup_datetime DESC) AS row_num
    FROM ingestion.trips t
    LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type_id = p.payment_type_id
)

SELECT
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    taxi_type,
    payment_type_name
FROM deduplicated_trips
WHERE row_num = 1;
