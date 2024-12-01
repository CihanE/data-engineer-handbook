-- 1. DDL for actors table
create type films as(
					film text,
					votes integer,
					rating real,
					filmid text
);

create type quality_class as ENUM('star', 'good', 'average', 'bad');

create table actors(
					actorid TEXT,
					actor TEXT,
					year INTEGER,
					films films[],
					quality_class quality_class,
					is_active BOOLEAN
);