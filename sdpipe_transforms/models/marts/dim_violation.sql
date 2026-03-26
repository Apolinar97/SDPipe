{{ config(materialized='table', unique_key='violation_key') }}

WITH distinct_violations AS (
    SELECT DISTINCT
        violation_section,
        violation_type,
        primary_violation_description
    FROM {{ ref('int_collisions_basic') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'violation_section',
        'violation_type',
        'primary_violation_description'
    ]) }} AS violation_key,
    violation_section,
    violation_type,
    primary_violation_description
FROM distinct_violations
