{{ config(materialized='table') }}

WITH stale_details AS (
    SELECT
        i.collision_detail_id,
        i.report_id,
        i.snapshot_dt,
        i.source_file,
        i.load_ts
    FROM {{ ref('int_collisions_details') }} i
    LEFT JOIN (
        SELECT {{ dbt_utils.generate_surrogate_key(['report_id', 'source_row_num']) }} AS collision_detail_id
        FROM {{ ref('stg_collisions_details') }}
    ) s ON i.collision_detail_id = s.collision_detail_id
    WHERE s.collision_detail_id IS NULL
),

stale_basic AS (
    SELECT
        i.report_id,
        i.snapshot_dt,
        i.source_file,
        i.load_ts
    FROM {{ ref('int_collisions_basic') }} i
    LEFT JOIN (
        SELECT DISTINCT report_id
        FROM {{ ref('stg_collisions_basic') }}
    ) s ON i.report_id = s.report_id
    WHERE s.report_id IS NULL
)

SELECT
    'collisions_details' AS model,
    collision_detail_id AS record_id,
    report_id,
    snapshot_dt,
    source_file,
    load_ts
FROM stale_details

UNION ALL

SELECT
    'collisions_basic' AS model,
    report_id AS record_id,
    report_id,
    snapshot_dt,
    source_file,
    load_ts
FROM stale_basic
