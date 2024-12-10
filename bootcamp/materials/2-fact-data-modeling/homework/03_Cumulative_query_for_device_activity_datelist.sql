insert into user_devices_cumulated
with dedupe_events as(
	select 
		user_id,
		browser_type,
		DATE(CAST(event_time as timestamp)) as date,
		row_number() 
			over(partition by user_id, browser_type, DATE(CAST(event_time as timestamp))) 
			as row_num
	from events e
	left join devices d
		on e.device_id = d.device_id
),
deduped_events as(
	select
		user_id,
		browser_type,
		date
	from dedupe_events e
	where row_num = 1
),
yesterday as(
	select *
	from user_devices_cumulated
	where date = DATE('2023-01-30')
),
today as(
	select
		cast(user_id as text),
		browser_type,
		date as date_active
	from deduped_events
	where date = DATE('2023-01-31')
		and user_id is not null
		and browser_type is not null
)
select
	coalesce(t.user_id, y.user_id) as user_id,
	coalesce(t.browser_type, y.browser_type) as browser_type,
	case when y.device_activity_datelist is null
		then ARRAY[t.date_active]
		when t.date_active is null
		then y.device_activity_datelist
		else ARRAY[t.date_active] || y.device_activity_datelist
		end as device_activity_datelist,
	coalesce(t.date_active, y.date + interval '1 day') as date
from today t
	full outer join yesterday y
	on t.user_id = y.user_id
	and t.browser_type = y.browser_type;
	