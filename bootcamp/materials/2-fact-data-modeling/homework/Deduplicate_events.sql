with deduped_events as(
	select 
		*,
		row_number() over(partition by user_id, device_id, event_time) as row_num
	from events
)
select *
from deduped_events
where row_num = 1