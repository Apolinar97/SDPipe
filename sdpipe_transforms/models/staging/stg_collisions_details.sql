SELECT
	-- Keys/Time
	report_id,
	date_time::timestamp as report_datetime,
	date_time::date as report_date,
	EXTRACT(HOUR FROM date_time) as report_hour,
	-- Collision participants
	NULLIF(TRIM(person_role), '') as person_role,
	NULLIF(TRIM(person_injury_lvl),'') as person_injury_level,
	NULLIF(TRIM(person_veh_type),'') as person_vehicle_type,
	NULLIF(TRIM(veh_type),'') as vehicle_type,
	NULLIF(TRIM(veh_make),'') as vehicle_make,
	NULLIF(TRIM(veh_model),'') as vehicle_model,
	-- Location
	police_beat,
	--Primary Address
	NULLIF(TRIM(address_no_primary),'') as street_number,
	NULLIF(TRIM(address_pd_primary),'') as street_direction,
	NULLIF(TRIM(address_road_primary),'') as street_name,
	NULLIF(TRIM(address_sfx_primary),'') as street_type,

	--Intersecting
	NULLIF(TRIM(address_pd_intersecting),'') as intersection_direction,
	NULLIF(TRIM(address_name_intersecting),'') as intersecting_street,
	NULLIF(TRIM(address_sfx_intersecting),'') as intersecting_street_type,

	-- Vioation Info
	NULLIF(TRIM(violation_section),'') as violation_section,
	NULLIF(TRIM(violation_type),'') as violation_type,
	NULLIF(TRIM(charge_desc),'') as primary_violation_description,

	-- Casulaties
	injured,
	killed,
	NULLIF(TRIM(hit_run_lvl),'') as hit_run_level,
	snapshot_dt,
	source_file,
	source_row_num::integer as source_row_num,
	load_ts
from {{source ('raw', 'collisions_details')}}
