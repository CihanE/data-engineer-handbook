-- 1. DDL for actors table
create type films as(
					film text,
					votes integer,
					rating real,
					filmid text
);

create type quality_class as ENUM('star', 'good', 'average', 'bad');

drop table actors;
create table actors(
					actorid TEXT,
					actor_name TEXT,
					year INTEGER,
					films films[],
					quality_class quality_class,
					is_active BOOLEAN
);

--2. Cumulative table generation query for actors table
insert into actors
with years as(
	select *
	from generate_series(1970,2021) as year
),
act as(
	select
		actorid,
		actor,
		MIN(year) as first_year
	from actor_films
	group by actorid, actor
),
act_years as(
	select * 
	from act a
	join years y
	on a.first_year <= y.year
),
windowed as(
	select
		ay.actorid,
		ay.actor,
		ay.year,
		array_remove(
			array_agg(
				case
					when af.year is not null
						then row(
							af.film,
							af.votes,
							af.rating,
							af.filmid
						)::films
				end)
			over (partition by ay.actorid order by coalesce(ay.year, af.year)),
			null
		) as films,
		af.year is not null as is_active
	from act_years ay
	left join actor_films af
		on ay.actorid = af.actorid
		and ay.year = af.year
	order by ay.actorid, ay.year
)
select
	w.actorid,
	w.actor,
	w.year,
	films,
	case
		when (films[cardinality(films)]::films).rating > 8 then 'star'
		when (films[cardinality(films)]::films).rating > 7 then 'good'
		when (films[cardinality(films)]::films).rating > 6 then 'average'
		else 'bad'
	end::quality_class as quality_class,
	w.is_active
from windowed w;
drop table actors_history_scd;
--3. DDL for actors_history_scd table
create table actors_history_scd (
								actor_name text,
								quality_class quality_class,
								is_active BOOLEAN,
								start_date INTEGER,
								end_date INTEGER,
								current_year INTEGER,
								primary key(actor_name, start_date, quality_class, is_active)
);

--4. Backfill query for actors_history_scd
insert into actors_history_scd
with add_previous as(
	select 
		actor_name,
		year,
		quality_class,
		is_active,
		LAG(quality_class, 1) over (partition by actor_name order by year) as previous_quality_class,
		LAG(is_active, 1) over (partition by actor_name order by year) as previous_is_active
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
			over (partition by actor_name order by year) as serie_identifier
from add_ind
)
select 
	actor_name,
	quality_class,
	is_active,
	MIN(year) as start_date,
	MAX(year) as end_date,
	2020 as current_year
from add_series
group by actor_name, serie_identifier, is_active, quality_class
order by actor_name, serie_identifier;

--5.Incremental query for actors_history_scd
create type scd_type as(
						quality_class quality_class,
						is_active BOOLEAN,
						start_date INTEGER,
						end_date INTEGER
);

with last_year_scd as(
	select * from actors_history_scd
	where current_year = 2020
	and end_date = 2020
),
historical_scd as(
	select
		actor_name,
		quality_class,
		is_active,
		start_date,
		end_date
	from actors_history_scd
	where current_year = 2020
	and end_date < 2020
),
this_year_data as(
	select * from actors
	where year = 2021
),
unchanged_records as(
	select
		ts.actor_name,
		ts.quality_class,
		ts.is_active,
		ls.start_date,
		ts.year as end_date
	from this_year_data ts
	join last_year_scd ls
	on ls.actor_name = ts.actor_name
	where ts.quality_class = ls.quality_class
	and ts.is_active = ls.is_active
),
changed_records as(
	select
		ts.actor_name,
		unnest(array[
			row(
				ls.quality_class,
				ls.is_active,
				ls.start_date,
				ls.end_date
				)::scd_type,
			row(
				ts.quality_class,
				ts.is_active,
				ts.year,
				ts.year
				)::scd_type
			]
		) as records
	from this_year_data ts
	left join last_year_scd ls
	on ts.actor_name = ls.actor_name
	where(ts.quality_class <> ls.quality_class
		or ts.is_active <> ls.is_active)
),
unnested_changed_records as(
	select
		actor_name,
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_date,
		(records::scd_type).end_date
	from changed_records
),
new_records as(
	select
		ts.actor_name,
		ts.quality_class,
		ts.is_active,
		ts.year as start_date,
		ts.year as end_date
	from this_year_data ts
	left join last_year_scd ls
		on ts.actor_name = ls.actor_name
	where ls.actor_name is null
)
select 
	*,
	2021 as current_year
from (
	select *
	from historical_scd
	union all
	select *
	from unchanged_records
	union all
	select *
	from unnested_changed_records
	union all
	select *
	from new_records) a;



	
	
					