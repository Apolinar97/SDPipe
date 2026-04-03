{{ config(unique_key='observation_id') }}

WITH base AS (
    SELECT *
    FROM {{ ref('stg_nws_observations') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'station_id',
        'observation_timestamp'
    ]) }} AS observation_id,
    station_id,
    station_name,
    observation_timestamp,
    observation_date,
    observation_hour,
    observation_timestamp_local,
    observation_date_local,
    observation_hour_local,
    latitude,
    longitude,
    text_description,
    temperature_c,
    wind_direction_deg,
    wind_speed_kmh,
    wind_gust_kmh,
    visibility_m,
    precip_last_1h_mm,
    precip_last_3h_mm,
    precip_last_6h_mm,
    cloud_cover,
    cloud_base_m,
    captured_at_utc,
    snapshot_dt,
    source_file,
    load_ts
FROM base
