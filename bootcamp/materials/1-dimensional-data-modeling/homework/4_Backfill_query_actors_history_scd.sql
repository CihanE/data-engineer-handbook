--4. Backfill query for actors_history_scd
insert into actors_history_scd
with add_previous as(
	select 
		actor,
		year,
		quality_class,
		is_active,
		LAG(quality_class, 1) over (partition by actor order by year) as previous_quality_class,
		LAG(is_active, 1) over (partition by actor order by year) as previous_is_active
	from actors
	where year <= 2020
),
add_ind as(
select *, 
		case 
			when quality_class <> previous_quality_class then 1
			when is_active <> previous_is_active then 1
			else 0
		end as change_ind
from add_previous
),
add_series AS(
select *,
		SUM(change_ind) 
			over (partition by actor order by year) as serie_identifier
from add_ind
)
select 
	actor,
	quality_class,
	is_active,
	MIN(year) as start_date,
	MAX(year) as end_date,
	2020 as current_year
from add_series
group by actor, serie_identifier, is_active, quality_class
order by actor , serie_identifier;