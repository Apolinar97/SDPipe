{{ config(materialized='table', unique_key='station_key') }}

WITH latest_per_station AS (
    SELECT DISTINCT ON (station_id)
        station_id,
        station_name,
        latitude,
        longitude
    FROM {{ ref('int_nws_observations') }}
    ORDER BY station_id ASC, observation_timestamp DESC
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['station_id']) }} AS station_key,
    station_id,
    station_name,
    latitude,
    longitude
FROM latest_per_station
