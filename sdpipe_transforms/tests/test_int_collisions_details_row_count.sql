-- Verify intermediate has at least as many rows as staging.
-- With incremental merge, intermediate can retain rows from previous loads,
-- so count should be >= staging. Fewer rows would indicate data loss.
SELECT
    staging_count,
    intermediate_count
FROM (
    SELECT
        (SELECT count(*) FROM {{ ref('stg_collisions_details') }}) AS staging_count,
        (SELECT count(*) FROM {{ ref('int_collisions_details') }}) AS intermediate_count
) AS counts
WHERE intermediate_count < staging_count
