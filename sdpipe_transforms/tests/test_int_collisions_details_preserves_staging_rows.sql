WITH staging_counts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'snapshot_dt',
            'source_file',
            'source_row_num'
        ]) }} AS collision_detail_id,
        count(*) AS staging_row_count
    FROM {{ ref('stg_collisions_details') }}
    GROUP BY 1
),
intermediate_counts AS (
    SELECT
        collision_detail_id,
        count(*) AS intermediate_row_count
    FROM {{ ref('int_collisions_details') }}
    GROUP BY 1
)
SELECT
    CASE
        WHEN staging_counts.collision_detail_id IS NULL THEN 'unexpected_in_intermediate'
        WHEN intermediate_counts.collision_detail_id IS NULL THEN 'missing_from_intermediate'
        ELSE 'row_count_mismatch'
    END AS issue_type,
    coalesce(staging_counts.collision_detail_id, intermediate_counts.collision_detail_id) AS collision_detail_id,
    coalesce(staging_row_count, 0) AS staging_row_count,
    coalesce(intermediate_row_count, 0) AS intermediate_row_count
FROM staging_counts
FULL OUTER JOIN intermediate_counts
    ON staging_counts.collision_detail_id = intermediate_counts.collision_detail_id
WHERE coalesce(staging_row_count, 0) <> coalesce(intermediate_row_count, 0)
