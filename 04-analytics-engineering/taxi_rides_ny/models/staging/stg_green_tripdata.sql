with source as (
    select *
    from {{ source('nytaxi', 'green_tripdata') }}
),

renamed as (
    select
        cast(vendorid as int64) as vendor_id,
        safe_cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
        safe_cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(ratecodeid as int64) as ratecode_id,
        cast(pulocationid as int64) as pickup_location_id,
        cast(dolocationid as int64) as dropoff_location_id,
        cast(passenger_count as int64) as passenger_count,
        cast(trip_distance as float64) as trip_distance,
        cast(fare_amount as float64) as fare_amount,
        cast(extra as float64) as extra,
        cast(mta_tax as float64) as mta_tax,
        cast(tip_amount as float64) as tip_amount,
        cast(tolls_amount as float64) as tolls_amount,
        cast(ehail_fee as float64) as ehail_fee,
        cast(improvement_surcharge as float64) as improvement_surcharge,
        cast(total_amount as float64) as total_amount,
        cast(payment_type as int64) as payment_type,
        cast(trip_type as int64) as trip_type,
        cast(congestion_surcharge as float64) as congestion_surcharge
    from source
)

select
    *,
    'green' as service_type
from renamed
where pickup_datetime >= timestamp('2019-01-01')
  and pickup_datetime < timestamp('2021-01-01')
