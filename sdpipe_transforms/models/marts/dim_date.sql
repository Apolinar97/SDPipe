{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="current_date + interval '1 day'"
    ) }}
),

dates AS (
    SELECT date_day::date AS date_day
    FROM date_spine
)

SELECT
    date_day,
    CASE
        WHEN EXTRACT(DOW FROM date_day) = 0 THEN 7
        ELSE EXTRACT(DOW FROM date_day)::integer
    END AS day_of_week,
    EXTRACT(DAY FROM date_day)::integer AS day_of_month,
    EXTRACT(MONTH FROM date_day)::integer AS month_number,
    EXTRACT(QUARTER FROM date_day)::integer AS quarter_number,
    EXTRACT(YEAR FROM date_day)::integer AS year_number,
    (TO_CHAR(date_day, 'YYYYMMDD'))::integer AS date_key,
    TRIM(TO_CHAR(date_day, 'Day')) AS day_of_week_name,
    TRIM(TO_CHAR(date_day, 'Month')) AS month_name,
    EXTRACT(DOW FROM date_day) IN (0, 6) AS is_weekend
FROM dates
