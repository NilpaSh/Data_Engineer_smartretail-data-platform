-- models/intermediate/int_product_metrics.sql
-- Aggregates sales performance per product

WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT order_id, order_date, order_status, is_completed
    FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_sales AS (
    SELECT
        i.product_id,
        COUNT(DISTINCT o.order_id)                           AS total_orders,
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.is_completed) AS completed_orders,
        SUM(i.quantity) FILTER (WHERE o.is_completed)        AS units_sold,
        SUM(i.line_total) FILTER (WHERE o.is_completed)      AS gross_revenue,
        AVG(i.effective_unit_price) FILTER (WHERE o.is_completed) AS avg_selling_price,
        MAX(o.order_date)                                    AS last_order_date,
        MIN(o.order_date)                                    AS first_order_date
    FROM items i
    JOIN orders o USING (order_id)
    GROUP BY i.product_id
),

with_margins AS (
    SELECT
        ps.*,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.cost_price,
        p.selling_price,
        p.margin_pct,
        p.margin_tier,
        p.stock_quantity,
        p.stock_status,

        -- Gross profit
        ROUND(ps.gross_revenue - (ps.units_sold * p.cost_price), 2) AS gross_profit,

        -- Revenue per unit sold
        ROUND(ps.gross_revenue / NULLIF(ps.units_sold, 0), 2)       AS revenue_per_unit,

        -- Sell-through rate (units sold vs. total stock available estimate)
        ROUND(
            100.0 * ps.units_sold / NULLIF(ps.units_sold + p.stock_quantity, 0)
        , 2)                                                          AS sell_through_rate_pct

    FROM product_sales ps
    JOIN products p USING (product_id)
)

SELECT * FROM with_margins
