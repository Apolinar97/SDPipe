{{ config(materialized='table', unique_key='beat') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['beat']) }} AS beat_key,
    {{ dbt_utils.generate_surrogate_key(['station_id']) }} AS station_key,
    beat,
    station_id,
    distance_to_station_km
FROM {{ ref('beat_station_mapping') }}
