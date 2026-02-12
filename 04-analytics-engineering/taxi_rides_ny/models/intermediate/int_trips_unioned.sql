select * from {{ ref('stg_yellow_tripdata') }}
union all
select * from {{ ref('stg_green_tripdata') }}
