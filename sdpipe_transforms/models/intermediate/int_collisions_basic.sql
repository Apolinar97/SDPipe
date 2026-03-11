{{ config(unique_key='report_id') }}

--Remove Duplicates
WITH
ROW_NUM_CTE AS (
    SELECT
        report_id,
        report_datetime,
        report_date,
        report_hour,
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
        is_fatal,
        has_injuries,
        is_hit_and_run,
        is_intersection_collision,
        snapshot_dt,
        source_file,
        load_ts,
        row_number() OVER (
            PARTITION BY
                report_id
            ORDER BY
                load_ts DESC,
                snapshot_dt DESC,
                source_file DESC
        ) AS rn
    FROM {{ ref('stg_collisions_basic') }}
)

SELECT
    report_id,
    report_datetime,
    report_date,
    report_hour,
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
    is_fatal,
    has_injuries,
    is_hit_and_run,
    is_intersection_collision,
    snapshot_dt,
    source_file,
    load_ts
FROM
    ROW_NUM_CTE
WHERE
    rn = 1
