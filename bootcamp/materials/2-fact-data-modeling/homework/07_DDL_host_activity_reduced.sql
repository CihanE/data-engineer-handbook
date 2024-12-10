create table host_activity_reduced(
	host_name text,
	month text,
	hit_array INT[],
	unique_visitors INT[],
	primary key(host_name, month)
);