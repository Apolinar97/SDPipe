{{ config(unique_key='collision_detail_id') }}

WITH details AS (
    SELECT * FROM {{ ref('int_collisions_details') }}
),

fingerprint_counts AS (
    SELECT
        report_id,
        collision_detail_fingerprint,
        COUNT(*) AS duplicate_group_size
    FROM details
    GROUP BY report_id, collision_detail_fingerprint
)

SELECT
    d.collision_detail_id,
    d.report_id,
    CAST(TO_CHAR(d.report_date, 'YYYYMMDD') AS INTEGER) AS date_key,
    {{ dbt_utils.generate_surrogate_key([
        'd.person_role',
        'd.person_injury_level',
        'd.person_vehicle_type',
        'd.vehicle_type',
        'd.vehicle_make',
        'd.vehicle_model'
    ]) }} AS person_key,
    fc.duplicate_group_size > 1 AS is_duplicate_participant,
    fc.duplicate_group_size
FROM details d
INNER JOIN fingerprint_counts fc
    ON d.report_id = fc.report_id
    AND d.collision_detail_fingerprint = fc.collision_detail_fingerprint
