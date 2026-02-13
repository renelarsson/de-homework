with yellow as (
	select
		vendor_id,
		pickup_datetime,
		dropoff_datetime,
		passenger_count,
		trip_distance,
		ratecode_id,
		store_and_fwd_flag,
		pickup_location_id,
		dropoff_location_id,
		payment_type,
		fare_amount,
		extra,
		mta_tax,
		tip_amount,
		tolls_amount,
		cast(null as float64) as ehail_fee,
		improvement_surcharge,
		total_amount,
		cast(null as int64) as trip_type,
		congestion_surcharge,
		airport_fee,
		service_type
	from {{ ref('stg_yellow_tripdata') }}
),

green as (
	select
		vendor_id,
		pickup_datetime,
		dropoff_datetime,
		passenger_count,
		trip_distance,
		ratecode_id,
		store_and_fwd_flag,
		pickup_location_id,
		dropoff_location_id,
		payment_type,
		fare_amount,
		extra,
		mta_tax,
		tip_amount,
		tolls_amount,
		ehail_fee,
		improvement_surcharge,
		total_amount,
		trip_type,
		congestion_surcharge,
		cast(null as float64) as airport_fee,
		service_type
	from {{ ref('stg_green_tripdata') }}
)

select * from yellow
union all
select * from green
