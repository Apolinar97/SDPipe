SELECT
    beat,
    NULLIF(TRIM(div), '') AS division,
    NULLIF(TRIM(serv), '') AS service_area,
    NULLIF(TRIM(name), '') AS neighborhood_name,
    snapshot_dt,
    source_file
FROM {{ source('raw', 'collisions_beats') }}
