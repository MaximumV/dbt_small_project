
SELECT 
taxi_id, 
date_trunc(date(trip_start_timestamp), month) as year_month_start,
sum(tips) as tips_sum_start,
FROM {{ source('public', 'taxi_trips') }}
where 1=1
and date_trunc(date(trip_start_timestamp), month) = '{{ var("start_date") }}'
GROUP BY 1,2
QUALIFY (ROW_NUMBER()OVER (ORDER BY tips_sum_start desc)) <=3