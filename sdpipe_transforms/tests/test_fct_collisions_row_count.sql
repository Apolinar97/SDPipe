-- Verify fct_collisions has at least as many rows as int_collisions_basic.
-- A LEFT JOIN means no rows should be dropped; fewer rows indicates a join issue.
SELECT
    basic_count,
    collisions_count
FROM (
    SELECT
        (SELECT count(*) FROM {{ ref('int_collisions_basic') }}) AS basic_count,
        (SELECT count(*) FROM {{ ref('fct_collisions') }}) AS collisions_count
) AS counts
WHERE collisions_count < basic_count
