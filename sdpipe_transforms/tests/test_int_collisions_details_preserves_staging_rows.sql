-- Only check that staging rows are present in intermediate
-- (extras from previous loads are expected with incremental materialization)
WITH staging AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['report_id', 'source_row_num']) }} AS collision_detail_id
    FROM {{ ref('stg_collisions_details') }}
),
intermediate AS (
    SELECT collision_detail_id
    FROM {{ ref('int_collisions_details') }}
)
SELECT
    s.collision_detail_id
FROM staging s
LEFT JOIN intermediate i ON s.collision_detail_id = i.collision_detail_id
WHERE i.collision_detail_id IS NULL
