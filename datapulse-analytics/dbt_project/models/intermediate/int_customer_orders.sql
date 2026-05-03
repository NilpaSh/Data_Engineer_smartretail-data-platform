-- models/intermediate/int_customer_orders.sql
-- Aggregates all order history per customer for downstream metrics

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

order_summary AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.is_completed,
        o.is_cancelled,
        o.is_returned,
        o.total_amount,
        COUNT(i.item_id)                    AS item_count,
        SUM(i.line_total)                   AS items_revenue,
        MAX(i.unit_price)                   AS max_item_price
    FROM orders o
    LEFT JOIN items i USING (order_id)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

customer_orders AS (
    SELECT
        customer_id,

        -- Volume metrics
        COUNT(order_id)                                  AS total_orders,
        COUNT(order_id) FILTER (WHERE is_completed)      AS completed_orders,
        COUNT(order_id) FILTER (WHERE is_cancelled)      AS cancelled_orders,
        COUNT(order_id) FILTER (WHERE is_returned)       AS returned_orders,

        -- Revenue metrics
        SUM(total_amount) FILTER (WHERE is_completed)    AS total_revenue,
        AVG(total_amount) FILTER (WHERE is_completed)    AS avg_order_value,
        MAX(total_amount) FILTER (WHERE is_completed)    AS max_order_value,

        -- Recency (for RFM)
        MAX(order_date) FILTER (WHERE is_completed)      AS last_order_date,
        MIN(order_date) FILTER (WHERE is_completed)      AS first_order_date,

        -- Frequency (days between first and last)
        (MAX(order_date) - MIN(order_date))
            FILTER (WHERE is_completed)                  AS customer_age_days,

        -- Items per order
        ROUND(AVG(item_count) FILTER (WHERE is_completed), 2) AS avg_items_per_order,

        -- Cancellation rate
        ROUND(
            100.0 * COUNT(order_id) FILTER (WHERE is_cancelled)
            / NULLIF(COUNT(order_id), 0)
        , 2)                                             AS cancellation_rate_pct

    FROM order_summary
    GROUP BY customer_id
)

SELECT * FROM customer_orders
