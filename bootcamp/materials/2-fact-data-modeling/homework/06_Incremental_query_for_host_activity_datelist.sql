insert into hosts_cumulated
with dedupe_events as(
	select 
		host,
		DATE(CAST(event_time as timestamp)) as date,
		row_number() 
			over(partition by host, DATE(CAST(event_time as timestamp))) 
				as row_num
	from events
),
deduped_events as(
	select *
	from dedupe_events
	where row_num = 1
),
yesterday as(
	select *
	from hosts_cumulated
	where date = DATE('2023-01-30')
),
today as(
	select
		cast(host as text),
		date as date_active
	from deduped_events
	where date = DATE('2023-01-31')
		and host is not null
)
select
	coalesce(t.host, y.host_name) as host_name,
	case when y.host_activity_datelist is null
		then ARRAY[t.date_active]
		when t.date_active is null
		then y.host_activity_datelist
		else ARRAY[t.date_active] || y.host_activity_datelist
		end as host_activity_datelist,
	coalesce(t.date_active, y.date + interval '1 day') as date
from today t
	full outer join yesterday y
	on t.host = y.host_name;
	