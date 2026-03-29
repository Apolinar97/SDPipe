{{ config(materialized='table', unique_key='beat_key') }}

WITH deduped_reference AS (
    SELECT
        beat,
        division,
        service_area,
        neighborhood_name,
        ROW_NUMBER() OVER (
            PARTITION BY beat
            ORDER BY neighborhood_name NULLS LAST
        ) AS rn
    FROM {{ ref('stg_collisions_beats') }}
),

reference_beats AS (
    SELECT
        beat,
        division,
        service_area,
        neighborhood_name
    FROM deduped_reference
    WHERE rn = 1
),

collision_beats AS (
    SELECT DISTINCT police_beat AS beat
    FROM {{ ref('int_collisions_basic') }}
),

orphan_beats AS (
    SELECT cb.beat
    FROM collision_beats AS cb
    LEFT JOIN reference_beats AS rb
        ON cb.beat = rb.beat
    WHERE rb.beat IS NULL
),

fallback_beats AS (
    SELECT
        beat,
        CASE WHEN beat = 999 THEN 'Unspecified' ELSE 'Unknown' END AS division,
        CASE WHEN beat = 999 THEN 'Unspecified' ELSE 'Unknown' END AS service_area,
        CASE WHEN beat = 999 THEN 'Unspecified' ELSE 'Unknown' END AS neighborhood_name
    FROM orphan_beats
),

all_beats AS (
    SELECT
        beat,
        division,
        service_area,
        neighborhood_name
    FROM reference_beats
    UNION ALL
    SELECT
        beat,
        division,
        service_area,
        neighborhood_name
    FROM fallback_beats
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['beat']) }} AS beat_key,
    beat,
    division,
    service_area,
    neighborhood_name
FROM all_beats
