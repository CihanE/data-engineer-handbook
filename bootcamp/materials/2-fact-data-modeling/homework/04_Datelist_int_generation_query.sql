with user_devices as(
	select *
	from user_devices_cumulated
	where date = DATE('2023-01-31')
),
series as(
	select *
	from generate_series(DATE('2023-01-01'), DATE('2023-01-31'), interval '1 day') as series_date
),
placeholder_ints as(
	select
		case when 
			device_activity_datelist @> ARRAY[DATE(series_date)]
			then 
			cast(POW(2, 32 - (date - DATE(series_date))) as BIGINT)
			else 0
		end as placeholder_int_value,
		*
	from user_devices 
		cross join series
)
select
	user_id,
	browser_type,
	cast(SUM(placeholder_int_value) as bigint) as datelist_int
from placeholder_ints
group by user_id, browser_type;