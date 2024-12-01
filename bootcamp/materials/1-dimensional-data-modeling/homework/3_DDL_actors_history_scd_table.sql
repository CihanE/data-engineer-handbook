--3. DDL for actors_history_scd table
create table actors_history_scd (
								actor text,
								quality_class quality_class,
								is_active BOOLEAN,
								start_date INTEGER,
								end_date INTEGER,
								current_year INTEGER,
								primary key(actor, quality_class, is_active, start_date)
);