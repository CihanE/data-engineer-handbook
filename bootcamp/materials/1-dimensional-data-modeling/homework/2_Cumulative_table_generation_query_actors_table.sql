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