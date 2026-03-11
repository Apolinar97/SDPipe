-- Verify int_collisions_basic has exactly one row per distinct report_id in staging.
-- After dedup, intermediate count should equal the number of distinct report_ids.
-- With incremental, intermediate may have extra reports from previous loads,
-- so we only check that all current staging report_ids are represented (count match).
SELECT
    staging_distinct_reports,
    intermediate_matching_reports
FROM (
    SELECT
        (SELECT count(DISTINCT report_id) FROM {{ ref('stg_collisions_basic') }}) AS staging_distinct_reports,
        (
            SELECT count(*)
            FROM {{ ref('int_collisions_basic') }} AS i
            WHERE
                EXISTS (
                    SELECT 1 FROM {{ ref('stg_collisions_basic') }} AS s
                    WHERE s.report_id = i.report_id
                )
        ) AS intermediate_matching_reports
) AS counts
WHERE staging_distinct_reports <> intermediate_matching_reports
