create table hosts_cumulated(
	host_name text,
	host_activity_datelist DATE[],
	date DATE,
	primary key (host_name, date)
)
	