create table user_devices_cumulated (
	user_id text,
	browser_type text,
-- the list of the dates in the past where user was active
	device_activity_datelist DATE[],
	date DATE,
	primary key (user_id, browser_type, date)
);


	
	
	
	