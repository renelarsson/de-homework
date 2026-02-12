select
  {{ dbt_utils.generate_surrogate_key([
    'service_type',
    'cast(vendor_id as string)',
    'cast(pickup_datetime as string)',
    'cast(dropoff_datetime as string)',
    'cast(pickup_location_id as string)',
    'cast(dropoff_location_id as string)'
  ]) }} as trip_id,
  service_type,
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  pickup_location_id,
  dropoff_location_id,
  passenger_count,
  trip_distance,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  airport_fee,
  ehail_fee,
  total_amount
from {{ ref('int_trips_unioned') }}
