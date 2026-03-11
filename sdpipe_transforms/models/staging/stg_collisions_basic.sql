SELECT
    --Basic WHEN info
    report_id,
    date_time::timestamp AS report_datetime,
    date_time::date AS report_date,
    police_beat,
    injured,
    --Primary Address
    killed,
    snapshot_dt,
    source_file,
    load_ts,

    --Intersecting
    EXTRACT(HOUR FROM date_time) AS report_hour,
    NULLIF(TRIM(address_no_primary), '') AS street_number,
    NULLIF(TRIM(address_pd_primary), '') AS street_direction,

    -- Vioation Info
    NULLIF(TRIM(address_road_primary), '') AS street_name,
    NULLIF(TRIM(address_sfx_primary), '') AS street_type,
    NULLIF(TRIM(address_pd_intersecting), '') AS intersection_direction,

    -- casualties
    NULLIF(TRIM(address_name_intersecting), '') AS intersecting_street,
    NULLIF(TRIM(address_sfx_intersecting), '') AS intersecting_street_type,
    NULLIF(TRIM(violation_section), '') AS violation_section,
    --Other
    NULLIF(TRIM(violation_type), '') AS violation_type,
    NULLIF(TRIM(charge_desc), '') AS primary_violation_description,
    NULLIF(TRIM(hit_run_lvl), '') AS hit_run_level,
    COALESCE(killed > 0, FALSE) AS is_fatal,
    COALESCE(injured > 0, FALSE) AS has_injuries,
    COALESCE(NULLIF(TRIM(hit_run_lvl), '') IS NOT NULL, FALSE) AS is_hit_and_run,
    COALESCE(NULLIF(TRIM(address_name_intersecting), '') IS NOT NULL, FALSE)
        AS is_intersection_collision
FROM {{ source ('raw', 'collisions_basic') }}
