-- Verify fct_collision_participants has at least as many rows as int_collisions_details.
-- The self-join for fingerprint counts should not drop any rows.
SELECT
    details_count,
    participants_count
FROM (
    SELECT
        (SELECT count(*) FROM {{ ref('int_collisions_details') }}) AS details_count,
        (SELECT count(*) FROM {{ ref('fct_collision_participants') }}) AS participants_count
) counts
WHERE participants_count < details_count
