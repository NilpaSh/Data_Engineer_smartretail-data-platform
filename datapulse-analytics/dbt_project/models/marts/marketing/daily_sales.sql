-- models/marts/marketing/daily_sales.sql
-- Pre-aggregated daily sales metrics for fast dashboard queries

WITH fact AS (
    SELECT * FROM {{ ref('fact_orders') }}
    WHERE is_completed = TRUE
)

SELECT
    order_date,
    order_year                          AS year,
    order_month_num                     AS month,
    month_name,
    order_week                          AS week,
    quarter_label,
    is_weekend,

    -- Volume
    COUNT(DISTINCT order_id)            AS total_orders,
    COUNT(DISTINCT customer_id)         AS unique_customers,
    SUM(quantity)                       AS units_sold,

    -- Revenue
    ROUND(SUM(gross_revenue), 2)        AS gross_revenue,
    ROUND(SUM(cogs), 2)                 AS total_cogs,
    ROUND(SUM(gross_profit), 2)         AS gross_profit,
    ROUND(AVG(gross_revenue) OVER (
        PARTITION BY order_id
    ), 2)                               AS avg_order_value,

    -- Running totals (YTD)
    ROUND(SUM(SUM(gross_revenue)) OVER (
        PARTITION BY order_year
        ORDER BY order_date
        ROWS UNBOUNDED PRECEDING
    ), 2)                               AS ytd_revenue,

    -- 7-day moving average
    ROUND(AVG(SUM(gross_revenue)) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                               AS revenue_7d_avg,

    -- By segment
    ROUND(SUM(gross_revenue) FILTER (WHERE customer_segment = 'B2C'), 2) AS b2c_revenue,
    ROUND(SUM(gross_revenue) FILTER (WHERE customer_segment = 'B2B'), 2) AS b2b_revenue,

    -- Top category of the day
    MODE() WITHIN GROUP (ORDER BY category) AS top_category

FROM fact
GROUP BY
    order_date, order_year, order_month_num, month_name,
    order_week, quarter_label, is_weekend
ORDER BY order_date
