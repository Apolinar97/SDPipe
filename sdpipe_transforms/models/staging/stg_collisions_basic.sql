SELECT 
	--Basic WHEN info
	report_id, 
	date_time::timestamp as report_datetime,
	date_time:: date as report_date,
	EXTRACT(HOUR FROM date_time) as report_hour,
	police_beat,
	--Primary Address
	address_no_primary as street_number,
	address_pd_primary as street_direction,
	address_road_primary as street_name,
	address_sfx_primary as street_type,

	--Intersecting
	address_pd_intersecting as intersection_direction,
	address_name_intersecting as intersecting_street,
	address_sfx_intersecting as intersecting_street_type,

	-- Vioation Info
	violation_section,
	violation_type,
	charge_desc as primary_violation_description,

	-- casualties
	injured,
	killed,
	hit_run_lvl as hit_run_level,

	--Other
	CASE WHEN killed >0 THEN TRUE ELSE FALSE END as is_fatal,
	CASE WHEN injured > 0 THEN TRUE ELSE FALSE END AS has_injuries,
	CASE WHEN hit_run_lvl IS NOT NULL THEN TRUE ELSE FALSE END AS is_hit_and_run,
	case when address_name_intersecting is not null 
         and address_name_intersecting <> '' 
         THEN TRUE ELSE FALSE END AS is_intersection_collision,
	snapshot_dt,
	source_file,
	load_ts
from {{source ('raw', 'collisions_basic')}}