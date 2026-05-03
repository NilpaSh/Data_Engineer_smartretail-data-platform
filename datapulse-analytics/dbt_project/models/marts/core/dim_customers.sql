-- models/marts/core/dim_customers.sql
-- Customer dimension with lifetime value and RFM segmentation

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_stats AS (
    SELECT * FROM {{ ref('int_customer_orders') }}
),

rfm_scores AS (
    SELECT
        customer_id,
        -- Recency score: 5 = bought recently, 1 = long ago
        NTILE(5) OVER (ORDER BY last_order_date DESC NULLS LAST)  AS recency_score,
        -- Frequency score: 5 = most orders
        NTILE(5) OVER (ORDER BY completed_orders ASC NULLS LAST)  AS frequency_score,
        -- Monetary score: 5 = highest revenue
        NTILE(5) OVER (ORDER BY total_revenue ASC NULLS LAST)     AS monetary_score
    FROM order_stats
),

rfm_segments AS (
    SELECT
        customer_id,
        recency_score,
        frequency_score,
        monetary_score,
        ROUND((recency_score + frequency_score + monetary_score) / 3.0, 2) AS rfm_avg,

        CASE
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4
                THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3
                THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2
                THEN 'New Customers'
            WHEN recency_score >= 3 AND frequency_score >= 2 AND monetary_score >= 2
                THEN 'Potential Loyalists'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3
                THEN 'At Risk'
            WHEN recency_score = 1 AND frequency_score >= 3
                THEN 'Cannot Lose Them'
            WHEN recency_score <= 2 AND frequency_score <= 2
                THEN 'Hibernating'
            ELSE 'Promising'
        END AS rfm_segment

    FROM rfm_scores
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.full_name,
    c.email,
    c.city,
    c.state,
    c.country,
    c.customer_segment,
    c.registration_date,

    -- Order stats
    COALESCE(os.total_orders, 0)        AS total_orders,
    COALESCE(os.completed_orders, 0)    AS completed_orders,
    COALESCE(os.cancelled_orders, 0)    AS cancelled_orders,
    COALESCE(os.total_revenue, 0)       AS lifetime_value,
    COALESCE(os.avg_order_value, 0)     AS avg_order_value,
    os.first_order_date,
    os.last_order_date,
    os.customer_age_days,
    COALESCE(os.avg_items_per_order, 0) AS avg_items_per_order,
    COALESCE(os.cancellation_rate_pct, 0) AS cancellation_rate_pct,

    -- RFM
    rfm.recency_score,
    rfm.frequency_score,
    rfm.monetary_score,
    rfm.rfm_avg,
    COALESCE(rfm.rfm_segment, 'No Orders') AS rfm_segment,

    -- Customer tier based on lifetime value
    CASE
        WHEN COALESCE(os.total_revenue, 0) >= 2000  THEN 'Platinum'
        WHEN COALESCE(os.total_revenue, 0) >= 1000  THEN 'Gold'
        WHEN COALESCE(os.total_revenue, 0) >= 300   THEN 'Silver'
        WHEN COALESCE(os.total_revenue, 0) > 0      THEN 'Bronze'
        ELSE                                              'Inactive'
    END AS customer_tier

FROM customers c
LEFT JOIN order_stats os USING (customer_id)
LEFT JOIN rfm_segments rfm USING (customer_id)
