WITH grouped AS (
    SELECT
        report_id,
        collision_detail_fingerprint,
        min(report_datetime) AS report_datetime,
        min(snapshot_dt) AS snapshot_dt,
        min(source_file) AS source_file,
        min(person_role) AS sample_person_role,
        min(person_injury_level) AS sample_person_injury_level,
        min(person_vehicle_type) AS sample_person_vehicle_type,
        min(vehicle_type) AS sample_vehicle_type,
        min(vehicle_make) AS sample_vehicle_make,
        min(vehicle_model) AS sample_vehicle_model,
        min(primary_violation_description) AS sample_primary_violation_description,
        count(*) AS repeated_row_count,
        min(source_row_num) AS first_source_row_num,
        max(source_row_num) AS last_source_row_num
    FROM {{ ref('int_collisions_details') }}
    GROUP BY
        report_id,
        collision_detail_fingerprint
)
SELECT
    report_id,
    collision_detail_fingerprint,
    report_datetime,
    snapshot_dt,
    source_file,
    sample_person_role,
    sample_person_injury_level,
    sample_person_vehicle_type,
    sample_vehicle_type,
    sample_vehicle_make,
    sample_vehicle_model,
    sample_primary_violation_description,
    repeated_row_count,
    first_source_row_num,
    last_source_row_num,
    repeated_row_count > 1 AS is_suspicious_repeat
FROM grouped
WHERE repeated_row_count > 1
