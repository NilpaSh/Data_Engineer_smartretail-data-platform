-- models/staging/stg_orders.sql
-- Staging model: cleans and enriches order records

WITH source AS (
    SELECT * FROM silver.orders
),

enriched AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        ship_date,
        days_to_ship,
        shipping_method,
        order_status,
        total_amount,
        NULLIF(TRIM(discount_code), '')    AS discount_code,

        -- Derived flags
        CASE WHEN order_status = 'completed' THEN TRUE ELSE FALSE END  AS is_completed,
        CASE WHEN order_status = 'cancelled' THEN TRUE ELSE FALSE END  AS is_cancelled,
        CASE WHEN order_status = 'returned'  THEN TRUE ELSE FALSE END  AS is_returned,

        -- Date parts for partitioning
        DATE_TRUNC('month', order_date)::DATE  AS order_month,
        DATE_TRUNC('week',  order_date)::DATE  AS order_week,
        EXTRACT(YEAR  FROM order_date)::INT    AS order_year,
        EXTRACT(MONTH FROM order_date)::INT    AS order_month_num,
        EXTRACT(DOW   FROM order_date)::INT    AS order_day_of_week,  -- 0=Sun

        _updated_at AS updated_at
    FROM source
)

SELECT * FROM enriched
