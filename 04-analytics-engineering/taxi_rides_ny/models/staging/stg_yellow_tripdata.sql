with source as (
    select *
    from {{ source('nytaxi', 'yellow_tripdata') }}
),

renamed as (
    select
        cast(vendorid as int64) as vendor_id,
        safe_cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        safe_cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
        cast(passenger_count as int64) as passenger_count,
        cast(trip_distance as float64) as trip_distance,
        cast(ratecodeid as int64) as ratecode_id,
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(pulocationid as int64) as pickup_location_id,
        cast(dolocationid as int64) as dropoff_location_id,
        cast(payment_type as int64) as payment_type,

        cast(fare_amount as float64) as fare_amount,
        cast(extra as float64) as extra,
        cast(mta_tax as float64) as mta_tax,
        cast(tip_amount as float64) as tip_amount,
        cast(tolls_amount as float64) as tolls_amount,
        cast(improvement_surcharge as float64) as improvement_surcharge,
        cast(total_amount as float64) as total_amount,
        cast(congestion_surcharge as float64) as congestion_surcharge,
        cast(airport_fee as float64) as airport_fee
    from source
)

select
    *,
    'yellow' as service_type
from renamed
where pickup_datetime >= timestamp('2019-01-01')
    and pickup_datetime < timestamp('2021-01-01')
