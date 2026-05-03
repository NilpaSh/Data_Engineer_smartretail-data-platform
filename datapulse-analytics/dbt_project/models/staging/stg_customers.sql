-- models/staging/stg_customers.sql
-- Staging model: selects & lightly renames Silver customers for dbt lineage

WITH source AS (
    SELECT * FROM silver.customers
),

renamed AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        city,
        state,
        country,
        zip_code,
        customer_segment,
        registration_date,
        _updated_at              AS updated_at
    FROM source
)

SELECT * FROM renamed
