-- models/staging/stg_products.sql

WITH source AS (
    SELECT * FROM silver.products
),

enriched AS (
    SELECT
        product_id,
        product_name,
        category,
        COALESCE(subcategory, 'Other')     AS subcategory,
        brand,
        cost_price,
        selling_price,
        margin_pct,
        stock_quantity,
        sku,

        -- Margin tier classification
        CASE
            WHEN margin_pct >= 60  THEN 'High Margin'
            WHEN margin_pct >= 40  THEN 'Medium Margin'
            WHEN margin_pct >= 20  THEN 'Low Margin'
            ELSE                        'Negative Margin'
        END AS margin_tier,

        -- Stock status
        CASE
            WHEN stock_quantity = 0    THEN 'Out of Stock'
            WHEN stock_quantity < 10   THEN 'Low Stock'
            WHEN stock_quantity < 50   THEN 'Normal Stock'
            ELSE                            'Well Stocked'
        END AS stock_status,

        _updated_at AS updated_at
    FROM source
)

SELECT * FROM enriched
