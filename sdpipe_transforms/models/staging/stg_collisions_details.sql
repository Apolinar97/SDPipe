SELECT
    -- Keys/Time
    report_id,
    date_time::timestamp AS report_datetime,
    date_time::date AS report_date,
    police_beat,
    -- Collision participants
    injured,
    killed,
    snapshot_dt,
    source_file,
    source_row_num::integer AS source_row_num,
    load_ts,
    -- Location
    EXTRACT(HOUR FROM date_time) AS report_hour,
    --Primary Address
    NULLIF(TRIM(person_role), '') AS person_role,
    NULLIF(TRIM(person_injury_lvl), '') AS person_injury_level,
    NULLIF(TRIM(person_veh_type), '') AS person_vehicle_type,
    NULLIF(TRIM(veh_type), '') AS vehicle_type,

    --Intersecting
    NULLIF(TRIM(veh_make), '') AS vehicle_make,
    NULLIF(TRIM(veh_model), '') AS vehicle_model,
    NULLIF(TRIM(address_no_primary), '') AS street_number,

    -- Vioation Info
    NULLIF(TRIM(address_pd_primary), '') AS street_direction,
    NULLIF(TRIM(address_road_primary), '') AS street_name,
    NULLIF(TRIM(address_sfx_primary), '') AS street_type,

    -- Casulaties
    NULLIF(TRIM(address_pd_intersecting), '') AS intersection_direction,
    NULLIF(TRIM(address_name_intersecting), '') AS intersecting_street,
    NULLIF(TRIM(address_sfx_intersecting), '') AS intersecting_street_type,
    NULLIF(TRIM(violation_section), '') AS violation_section,
    NULLIF(TRIM(violation_type), '') AS violation_type,
    NULLIF(TRIM(charge_desc), '') AS primary_violation_description,
    NULLIF(TRIM(hit_run_lvl), '') AS hit_run_level
FROM {{ source ('raw', 'collisions_details') }}
