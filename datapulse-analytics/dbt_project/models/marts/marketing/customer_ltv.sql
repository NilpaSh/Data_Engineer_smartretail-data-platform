-- models/marts/marketing/customer_ltv.sql
-- Customer Lifetime Value model with cohort analysis & churn prediction signals

WITH customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

monthly_revenue AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', order_date)::DATE   AS revenue_month,
        SUM(gross_revenue)                       AS monthly_revenue,
        COUNT(DISTINCT order_id)                 AS monthly_orders
    FROM {{ ref('fact_orders') }}
    WHERE is_completed = TRUE
    GROUP BY 1, 2
),

cohort_base AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', registration_date)::DATE AS cohort_month
    FROM customers
),

cohort_activity AS (
    SELECT
        cb.cohort_month,
        mr.revenue_month,
        DATE_PART('month', AGE(mr.revenue_month, cb.cohort_month))::INT AS months_since_signup,
        COUNT(DISTINCT mr.customer_id)                                    AS active_customers,
        SUM(mr.monthly_revenue)                                           AS cohort_revenue
    FROM cohort_base cb
    LEFT JOIN monthly_revenue mr USING (customer_id)
    WHERE mr.revenue_month IS NOT NULL
    GROUP BY 1, 2, 3
),

cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size
    FROM cohort_base
    GROUP BY 1
),

-- Predict churn: customers who haven't ordered in > 90 days
churn_signals AS (
    SELECT
        customer_id,
        CASE
            WHEN last_order_date IS NULL THEN 'Never Purchased'
            WHEN CURRENT_DATE - last_order_date > 365 THEN 'Churned'
            WHEN CURRENT_DATE - last_order_date > 180 THEN 'At Risk of Churn'
            WHEN CURRENT_DATE - last_order_date > 90  THEN 'Needs Attention'
            ELSE 'Active'
        END AS churn_status,
        CURRENT_DATE - last_order_date AS days_since_last_order
    FROM customers
),

ltv_final AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.customer_segment,
        c.state,
        c.registration_date,
        c.rfm_segment,
        c.customer_tier,
        c.total_orders,
        c.completed_orders,
        c.lifetime_value,
        c.avg_order_value,
        c.first_order_date,
        c.last_order_date,
        c.customer_age_days,

        -- Projected annual value (based on avg order freq * AOV)
        CASE
            WHEN c.customer_age_days > 30 AND c.completed_orders > 1
            THEN ROUND(
                (c.completed_orders::FLOAT / NULLIF(c.customer_age_days, 0))
                * 365 * c.avg_order_value, 2
            )
            ELSE c.avg_order_value
        END AS projected_annual_value,

        cs.churn_status,
        cs.days_since_last_order

    FROM customers c
    LEFT JOIN churn_signals cs USING (customer_id)
)

SELECT * FROM ltv_final
