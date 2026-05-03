-- models/marts/core/fact_orders.sql
-- Central fact table — one row per order line item

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT customer_id, customer_segment, state, rfm_segment, customer_tier
    FROM {{ ref('dim_customers') }}
),

products AS (
    SELECT product_id, category, subcategory, brand, cost_price, margin_pct
    FROM {{ ref('dim_products') }}
),

dates AS (
    SELECT date_id, date, year, quarter, month, week_of_year,
           month_name, day_of_week, is_weekend
    FROM {{ ref('dim_date') }}
),

joined AS (
    SELECT
        -- Keys
        oi.item_id,
        oi.order_id,
        oi.product_id,
        o.customer_id,
        d.date_id,

        -- Order attributes
        o.order_date,
        o.order_status,
        o.is_completed,
        o.is_cancelled,
        o.is_returned,
        o.shipping_method,
        o.discount_code,
        o.days_to_ship,
        o.order_month,
        o.order_week,
        o.order_year,
        o.order_month_num,
        o.order_day_of_week,

        -- Product attributes
        p.category,
        p.subcategory,
        p.brand,
        p.cost_price             AS product_cost,
        p.margin_pct             AS product_margin_pct,

        -- Customer attributes
        c.customer_segment,
        c.state                  AS customer_state,
        c.rfm_segment,
        c.customer_tier,

        -- Date attributes
        d.year,
        d.quarter,
        d.quarter_label,
        d.month,
        d.month_name,
        d.week_of_year,
        d.is_weekend,

        -- Measures
        oi.quantity,
        oi.unit_price,
        oi.discount_pct,
        oi.line_total            AS gross_revenue,

        -- COGS and profit
        ROUND(oi.quantity * p.cost_price, 2)            AS cogs,
        ROUND(oi.line_total - (oi.quantity * p.cost_price), 2) AS gross_profit

    FROM order_items oi
    JOIN orders     o  USING (order_id)
    JOIN products   p  USING (product_id)
    LEFT JOIN customers c USING (customer_id)
    LEFT JOIN dates d ON d.date = o.order_date
)

SELECT * FROM joined
