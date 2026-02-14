with source as (
    select *
    from {{ source('nytaxi', 'fhv_tripdata_2019') }}
),

renamed as (
    select
        cast(dispatching_base_num as string) as dispatching_base_num,
        safe_cast(pickup_datetime as timestamp) as pickup_datetime,
        safe_cast(dropOff_datetime as timestamp) as dropoff_datetime,
        cast(PUlocationID as int64) as pickup_location_id,
        cast(DOlocationID as int64) as dropoff_location_id,
        cast(SR_Flag as int64) as sr_flag,
        cast(Affiliated_base_number as string) as affiliated_base_number
    from source
    where dispatching_base_num is not null
)

select *
from renamed
