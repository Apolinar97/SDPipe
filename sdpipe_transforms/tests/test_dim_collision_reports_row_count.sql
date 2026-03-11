-- Verify dim_collision_reports has at least as many rows as int_collisions_basic.
-- A LEFT JOIN means no rows should be dropped; fewer rows indicates a join issue.
SELECT
    basic_count,
    reports_count
FROM (
    SELECT
        (SELECT count(*) FROM {{ ref('int_collisions_basic') }}) AS basic_count,
        (SELECT count(*) FROM {{ ref('dim_collision_reports') }}) AS reports_count
) AS counts
WHERE reports_count < basic_count
