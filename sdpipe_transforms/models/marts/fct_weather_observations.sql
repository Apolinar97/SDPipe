{{ config(unique_key='observation_id') }}

WITH obs AS (
    SELECT * FROM {{ ref('int_nws_observations') }}
)

SELECT
    observation_id,
    {{ dbt_utils.generate_surrogate_key(['station_id']) }} AS station_key,
    CAST(TO_CHAR(observation_date_local, 'YYYYMMDD') AS INTEGER) AS date_key,
    observation_timestamp,
    observation_hour,
    observation_timestamp_local,
    observation_date_local,
    observation_hour_local,
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
    text_description
FROM obs
