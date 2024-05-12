{{ config(
    materialized='incremental',
		incremental_strategy = 'insert_overwrite',
    partition_by={
      "field": "year_month",
      "data_type": "date",
    }
) }}

with start_month as (
    SELECT *
    FROM {{ ref('stg_top3_tips_taxi') }}
),

next_month as (
	SELECT 
		taxi_id, 
		date_trunc(date(trip_start_timestamp), month) as year_month,
		sum(tips) as tips_sum,
	FROM {{ source('public', 'taxi_trips') }}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run  
  WHERE DATE(year_month) >= (SELECT MAX(DATE(year_month)) FROM {{ this }})
{% endif %}
		GROUP BY 1,2
),

joined_months as (
  SELECT 
		next_month.taxi_id,
		year_month,
		tips_sum,
		LAG(tips_sum) OVER(PARTITION BY next_month.taxi_id ORDER BY year_month) as prev_tips_sum
  FROM next_month
  JOIN start_month ON start_month.taxi_id=next_month.taxi_id
		AND start_month.year_month_start<=next_month.year_month
)

SELECT 
	taxi_id,
	year_month,
	tips_sum,
	ROUND(SAFE_divide(tips_sum,prev_tips_sum),2) as tips_change
FROM joined_months
WHERE year_month>= date_trunc('{{ var("start_date") }}'+32, month)   --'2018-05-01'
