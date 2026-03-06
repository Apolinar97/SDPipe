WITH basic AS (
    SELECT * FROM {{ ref('int_collisions_basic') }}
),

participant_counts AS (
    SELECT
        report_id,
        COUNT(*) AS participant_count
    FROM {{ ref('int_collisions_details') }}
    GROUP BY report_id
)

SELECT
    b.report_id,
    {{ dbt_utils.generate_surrogate_key([
        'b.violation_section',
        'b.violation_type',
        'b.primary_violation_description'
    ]) }} AS violation_key,
    b.report_date,
    b.report_datetime,
    b.report_hour,
    b.police_beat,
    b.street_number,
    b.street_direction,
    b.street_name,
    b.street_type,
    b.intersection_direction,
    b.intersecting_street,
    b.intersecting_street_type,
    b.violation_section,
    b.violation_type,
    b.primary_violation_description,
    b.injured,
    b.killed,
    COALESCE(pc.participant_count, 0) AS participant_count,
    b.hit_run_level,
    b.is_fatal,
    b.has_injuries,
    b.is_hit_and_run,
    b.is_intersection_collision
FROM basic b
LEFT JOIN participant_counts pc
    ON b.report_id = pc.report_id
