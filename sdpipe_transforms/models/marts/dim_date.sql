WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="current_date + interval '1 day'"
    ) }}
),

dates AS (
    SELECT
        date_day::date AS date_day
    FROM date_spine
)

SELECT
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) AS date_key,
    date_day,
    EXTRACT(DOW FROM date_day)::INTEGER AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_of_week_name,
    EXTRACT(DAY FROM date_day)::INTEGER AS day_of_month,
    EXTRACT(MONTH FROM date_day)::INTEGER AS month_number,
    TO_CHAR(date_day, 'Month') AS month_name,
    EXTRACT(QUARTER FROM date_day)::INTEGER AS quarter_number,
    EXTRACT(YEAR FROM date_day)::INTEGER AS year_number,
    EXTRACT(DOW FROM date_day) IN (0, 6) AS is_weekend
FROM dates
