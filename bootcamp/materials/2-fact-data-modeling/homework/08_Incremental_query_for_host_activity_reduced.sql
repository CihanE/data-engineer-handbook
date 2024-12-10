insert into host_activity_reduced
with daily_aggregate as(
	select
		host,
		date(event_time) as date,
		count(1) as num_hits,
		count(distinct(user_id)) as num_unique_visits
	from events
	where date(event_time) = date('2023-01-31')
	group by host, date(event_time)
),
yesterday_array as(
	select *
	from host_activity_reduced
	where month = '2023-01'
)
select
	coalesce(da.host, ya.host_name) as host_name,
	coalesce(ya.month, to_char(da.date, 'YYYY-MM')) as month,
	case when ya.hit_array is not null
		then ya.hit_array || ARRAY[coalesce(da.num_hits, 0)]
		when ya.hit_array is null
		then array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)])
			|| array[coalesce(da.num_hits, 0)]
	end as hit_array,
		case when ya.unique_visitors is not null
		then ya.unique_visitors || ARRAY[coalesce(da.num_unique_visits, 0)]
		when ya.unique_visitors is null
		then array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)])
			|| array[coalesce(da.num_unique_visits, 0)]
	end as unique_visitors
from daily_aggregate da
	full outer join yesterday_array ya
	on da.host = ya.host_name
on conflict (host_name, month)
do update set hit_array = excluded.hit_array, 
	unique_visitors = excluded.unique_visitors;