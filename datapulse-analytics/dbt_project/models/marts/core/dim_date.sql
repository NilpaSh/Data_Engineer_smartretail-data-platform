-- models/marts/core/dim_date.sql
-- Date dimension: spine of all time-series analysis

WITH date_spine AS (
    SELECT
        generate_series('2023-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL)::DATE AS date
),

enriched AS (
    SELECT
        TO_CHAR(date, 'YYYYMMDD')::INT          AS date_id,
        date,
        EXTRACT(YEAR  FROM date)::INT           AS year,
        EXTRACT(QUARTER FROM date)::INT         AS quarter,
        'Q' || EXTRACT(QUARTER FROM date)::TEXT AS quarter_label,
        EXTRACT(MONTH FROM date)::INT           AS month,
        TO_CHAR(date, 'Mon')                    AS month_short,
        TO_CHAR(date, 'Month')                  AS month_name,
        EXTRACT(WEEK  FROM date)::INT           AS week_of_year,
        EXTRACT(DOW   FROM date)::INT           AS day_of_week,  -- 0=Sunday
        TO_CHAR(date, 'Day')                    AS day_name,
        EXTRACT(DAY   FROM date)::INT           AS day_of_month,
        EXTRACT(DOY   FROM date)::INT           AS day_of_year,

        -- Useful flags
        CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        DATE_TRUNC('month', date)::DATE         AS first_day_of_month,
        (DATE_TRUNC('month', date) + INTERVAL '1 month - 1 day')::DATE AS last_day_of_month,
        DATE_TRUNC('quarter', date)::DATE       AS first_day_of_quarter,
        DATE_TRUNC('year', date)::DATE          AS first_day_of_year,

        -- Relative periods
        CASE WHEN date >= DATE_TRUNC('month', CURRENT_DATE) THEN TRUE ELSE FALSE END AS is_current_month,
        CASE WHEN date >= DATE_TRUNC('year',  CURRENT_DATE) THEN TRUE ELSE FALSE END AS is_current_year

    FROM date_spine
)

SELECT * FROM enriched
