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
)
select * from deduped_events;


