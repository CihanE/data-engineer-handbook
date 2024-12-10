with deduped_devices as(
	select 
		*,
		row_number() over(partition by device_id, browser_type) as row_num
	from devices
)
select *
from deduped_devices
where row_num = 1