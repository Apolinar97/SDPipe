WITH base AS (
    SELECT DISTINCT ON (station_id, observation_timestamp)  -- noqa: LT09
        *
    FROM {{ source('raw', 'nws_observations') }}
    ORDER BY
        station_id ASC,
        observation_timestamp ASC,
        snapshot_dt DESC,
        load_ts DESC
)

SELECT
    -- Station
    station_id,
    station_name,

    -- Observation time
    observation_timestamp,

    -- Location
    latitude,
    longitude,

    -- Measurements (already unit-converted and QC-filtered by flattener)
    temperature_c,
    wind_direction_deg,
    wind_speed_kmh,
    wind_gust_kmh,
    visibility_m,
    precip_last_1h_mm,
    precip_last_3h_mm,
    precip_last_6h_mm,

    -- Cloud
    cloud_base_m,

    -- Audit
    captured_at_utc,
    snapshot_dt,
    source_file,
    load_ts,

    -- Derived (UTC)
    observation_timestamp::date AS observation_date,
    EXTRACT(HOUR FROM observation_timestamp) AS observation_hour,

    -- Derived (San Diego local time)
    observation_timestamp AT TIME ZONE 'America/Los_Angeles'
        AS observation_timestamp_local,
    (observation_timestamp AT TIME ZONE 'America/Los_Angeles')::date
        AS observation_date_local,
    EXTRACT(
        HOUR FROM observation_timestamp AT TIME ZONE 'America/Los_Angeles'
    ) AS observation_hour_local,
    NULLIF(TRIM(text_description), '') AS text_description,
    NULLIF(TRIM(cloud_cover), '') AS cloud_cover

FROM base
