-- models/marts/core/dim_products.sql
-- Product dimension enriched with performance metrics

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

metrics AS (
    SELECT * FROM {{ ref('int_product_metrics') }}
)

SELECT
    p.product_id,
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
    p.sku,

    -- Performance (NULL-safe defaults for products with no sales)
    COALESCE(m.total_orders, 0)          AS total_orders,
    COALESCE(m.units_sold, 0)            AS units_sold,
    COALESCE(m.gross_revenue, 0)         AS gross_revenue,
    COALESCE(m.gross_profit, 0)          AS gross_profit,
    COALESCE(m.avg_selling_price, p.selling_price) AS avg_selling_price,
    COALESCE(m.sell_through_rate_pct, 0) AS sell_through_rate_pct,
    m.last_order_date,
    m.first_order_date,

    -- Performance tier
    CASE
        WHEN COALESCE(m.gross_revenue, 0) >= 10000 THEN 'Top Performer'
        WHEN COALESCE(m.gross_revenue, 0) >= 3000  THEN 'Strong Performer'
        WHEN COALESCE(m.gross_revenue, 0) >= 500   THEN 'Average Performer'
        WHEN COALESCE(m.gross_revenue, 0) > 0      THEN 'Low Performer'
        ELSE                                             'No Sales'
    END AS performance_tier

FROM products p
LEFT JOIN metrics m USING (product_id)
