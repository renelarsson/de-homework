with trips as (
  select
    service_type,
    date_trunc(date(pickup_datetime), month) as revenue_month,
    pickup_location_id,
    sum(total_amount) as revenue_monthly_total_amount,
    count(*) as total_monthly_trips
  from {{ ref('fct_trips') }}
  group by 1,2,3
)

select
  t.service_type,
  t.revenue_month,
  t.pickup_location_id,
  z.zone as pickup_zone,
  z.borough as pickup_borough,
  t.revenue_monthly_total_amount,
  t.total_monthly_trips
from trips t
left join {{ ref('dim_zones') }} z
  on t.pickup_location_id = z.location_id
