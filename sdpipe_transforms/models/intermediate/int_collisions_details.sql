{{ config(unique_key='collision_detail_id') }}

WITH base AS (
    SELECT
        *
    FROM {{ ref('stg_collisions_details') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key([
        'report_id',
        'source_row_num'
    ]) }} AS collision_detail_id,
    {{ dbt_utils.generate_surrogate_key([
        'report_id',
        'report_datetime',
        'person_role',
        'person_injury_level',
        'person_vehicle_type',
        'vehicle_type',
        'vehicle_make',
        'vehicle_model',
        'police_beat',
        'street_number',
        'street_direction',
        'street_name',
        'street_type',
        'intersection_direction',
        'intersecting_street',
        'intersecting_street_type',
        'violation_section',
        'violation_type',
        'primary_violation_description',
        'injured',
        'killed',
        'hit_run_level'
    ]) }} AS collision_detail_fingerprint,
    report_id,
    report_datetime,
    report_date,
    report_hour,
    person_role,
    person_injury_level,
    person_vehicle_type,
    vehicle_type,
    vehicle_make,
    vehicle_model,
    police_beat,
    street_number,
    street_direction,
    street_name,
    street_type,
    intersection_direction,
    intersecting_street,
    intersecting_street_type,
    violation_section,
    violation_type,
    primary_violation_description,
    injured,
    killed,
    hit_run_level,
    snapshot_dt,
    source_file,
    source_row_num,
    load_ts
FROM base
