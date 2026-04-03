{{ config(unique_key='report_id') }}

WITH basic AS (
    SELECT * FROM {{ ref('int_collisions_basic') }}
),

participant_counts AS (
    SELECT
        report_id,
        COUNT(*) AS participant_count
    FROM {{ ref('int_collisions_details') }}
    GROUP BY report_id
),

collision_stations AS (
    SELECT
        b.report_id,
        b.report_hour,
        CAST(TO_CHAR(b.report_date, 'YYYYMMDD') AS INTEGER) AS date_key,
        bs.station_key,
        bs.distance_to_station_km
    FROM {{ ref('int_collisions_basic') }} AS b
    LEFT JOIN {{ ref('map_beat_station') }} AS bs
        ON {{ dbt_utils.generate_surrogate_key(['b.police_beat']) }} = bs.beat_key
),

hourly_weather AS (
    SELECT DISTINCT ON (station_key, date_key, observation_hour_local)
        observation_id,
        station_key,
        date_key,
        observation_hour_local,
        observation_timestamp,
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
    FROM {{ ref('fct_weather_observations') }}
    ORDER BY station_key, date_key, observation_hour_local, observation_timestamp
)

SELECT
    b.report_id,
    CAST(TO_CHAR(b.report_date, 'YYYYMMDD') AS INTEGER) AS date_key,
    {{ dbt_utils.generate_surrogate_key([
        'b.violation_section',
        'b.violation_type',
        'b.primary_violation_description'
    ]) }} AS violation_key,
    {{ dbt_utils.generate_surrogate_key(['b.police_beat']) }} AS beat_key,
    b.report_date,
    b.report_datetime,
    b.report_hour,
    b.police_beat,
    b.street_number,
    b.street_direction,
    b.street_name,
    b.street_type,
    b.intersection_direction,
    b.intersecting_street,
    b.intersecting_street_type,
    b.violation_section,
    b.violation_type,
    b.primary_violation_description,
    b.injured,
    b.killed,
    COALESCE(pc.participant_count, 0) AS participant_count,
    b.hit_run_level,
    b.is_fatal,
    b.has_injuries,
    b.is_hit_and_run,
    b.is_intersection_collision,
    cs.station_key,
    cs.distance_to_station_km,
    hw.observation_id,
    hw.observation_hour_local,
    hw.temperature_c,
    hw.wind_direction_deg,
    hw.wind_speed_kmh,
    hw.wind_gust_kmh,
    hw.visibility_m,
    hw.precip_last_1h_mm,
    hw.precip_last_3h_mm,
    hw.precip_last_6h_mm,
    hw.cloud_cover,
    hw.cloud_base_m,
    hw.text_description AS weather_description
FROM basic AS b
LEFT JOIN participant_counts AS pc
    ON b.report_id = pc.report_id
LEFT JOIN collision_stations AS cs
    ON b.report_id = cs.report_id
LEFT JOIN hourly_weather AS hw
    ON
        cs.station_key = hw.station_key
        AND cs.date_key = hw.date_key
        AND b.report_hour = hw.observation_hour_local
