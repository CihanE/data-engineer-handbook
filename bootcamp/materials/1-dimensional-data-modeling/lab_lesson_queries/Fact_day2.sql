select
	max(event_time),
	min(event_time)
from events;

drop table users_cumulated;
create table users_cumulated(
	user_id TEXT,
-- the list of the dates in the past where user was active
	dates_active DATE[],
-- current date for the user
	date DATE,
	primary key (user_id, date)
);

insert into users_cumulated
with yesterday as (
	select
		*
	from users_cumulated
	where date = DATE('2023-01-30')
),
	today as (
	select
		cast(user_id as text),
		DATE(CAST(event_time as timestamp)) as date_active
	from events
	where 
		DATE(CAST(event_time as timestamp)) = DATE('2023-01-31')
		and user_id is not null
	group by user_id, DATE(CAST(event_time as timestamp))
)
select 
	COALESCE(t.user_id, y.user_id) as user_id,
	case when y.dates_active is null
		then ARRAY[t.date_active]
		when t.date_active is null then y.dates_active
		else ARRAY[t.date_active] || y.dates_active
		end as dates_active,
	coalesce(t.date_active, y.date + interval '1 day') as date 
from today t
	full outer join yesterday y
	on t.user_id = y.user_id;
	
select * from users_cumulated
where date = date('2023-01-31');


with users as (
	select * from users_cumulated
	where date = date('2023-01-31')	
),
	series as(
	select *
	from generate_series(DATE('2023-01-01'),DATE('2023-01-31'), interval '1 day') as series_date
),
	placeholder_ints as(
	select
		CASE WHEN
			dates_active @> array[DATE(series_date)]
			then
			cast(POW( 2, 32- (date - DATE(series_date))) as BIGINT)
			else 0 
		end as placeholder_int_value,
		*
	from users cross join series
)
select 
	user_id,
	cast(cast(SUM(placeholder_int_value) as bigint) as bit(32)),
	bit_count(cast(cast(SUM(placeholder_int_value) as bigint) as bit(32))) 
		as dim_is_monthly_active,
	bit_count(CAST('11111110000000000000000000000000' as bit(32)) & 
		cast(cast(SUM(placeholder_int_value) as bigint) as bit(32)))
		as dim_is_weekly_active,
	bit_count(CAST('10000000000000000000000000000000' as bit(32)) & 
		cast(cast(SUM(placeholder_int_value) as bigint) as bit(32)))
		as dim_is_daily_active
from placeholder_ints
group by user_id;