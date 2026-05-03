-- models/staging/stg_order_items.sql

WITH source AS (
    SELECT * FROM silver.order_items
)

SELECT
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    line_total,
    ROUND(line_total / NULLIF(quantity, 0), 2)  AS effective_unit_price,
    _updated_at AS updated_at
FROM source
