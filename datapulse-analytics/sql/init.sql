-- =============================================================================
-- DataPulse Retail Analytics Platform — Database Schema
-- Medallion Architecture: Bronze → Silver → Gold
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- SCHEMAS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS bronze;   -- Raw data as-is from sources
CREATE SCHEMA IF NOT EXISTS silver;   -- Cleaned, deduplicated, typed
CREATE SCHEMA IF NOT EXISTS gold;     -- Analytics-ready dimensions & facts

-- ─────────────────────────────────────────────────────────────────────────────
-- BRONZE LAYER — Raw ingested tables
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze.raw_customers (
    id                  SERIAL PRIMARY KEY,
    customer_id         VARCHAR(50),
    first_name          TEXT,
    last_name           TEXT,
    email               TEXT,
    phone               TEXT,
    city                TEXT,
    state               TEXT,
    country             TEXT,
    zip_code            TEXT,
    customer_segment    TEXT,           -- 'B2B' or 'B2C'
    registration_date   TEXT,           -- raw string, typed in Silver
    _source_file        TEXT,
    _ingested_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.raw_products (
    id              SERIAL PRIMARY KEY,
    product_id      VARCHAR(50),
    product_name    TEXT,
    category        TEXT,
    subcategory     TEXT,
    brand           TEXT,
    cost_price      TEXT,               -- raw string
    selling_price   TEXT,               -- raw string
    stock_quantity  TEXT,
    sku             TEXT,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.raw_orders (
    id              SERIAL PRIMARY KEY,
    order_id        VARCHAR(50),
    customer_id     VARCHAR(50),
    order_date      TEXT,
    ship_date       TEXT,
    shipping_method TEXT,
    order_status    TEXT,
    total_amount    TEXT,
    discount_code   TEXT,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.raw_order_items (
    id              SERIAL PRIMARY KEY,
    item_id         VARCHAR(50),
    order_id        VARCHAR(50),
    product_id      VARCHAR(50),
    quantity        TEXT,
    unit_price      TEXT,
    discount_pct    TEXT,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.raw_events (
    id              SERIAL PRIMARY KEY,
    event_id        VARCHAR(50),
    customer_id     VARCHAR(50),
    event_type      TEXT,               -- 'page_view', 'add_to_cart', 'purchase'
    product_id      VARCHAR(50),
    event_timestamp TEXT,
    session_id      TEXT,
    device_type     TEXT,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP DEFAULT NOW()
);

-- Pipeline run log
CREATE TABLE IF NOT EXISTS bronze.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100),
    run_date        DATE,
    status          VARCHAR(20),        -- 'success', 'failed', 'running'
    rows_processed  INTEGER,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    error_message   TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- SILVER LAYER — Cleaned & typed tables
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id         VARCHAR(50) PRIMARY KEY,
    first_name          TEXT NOT NULL,
    last_name           TEXT NOT NULL,
    full_name           TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    email               TEXT,
    phone               TEXT,
    city                TEXT,
    state               TEXT,
    country             TEXT,
    zip_code            TEXT,
    customer_segment    VARCHAR(10),
    registration_date   DATE,
    _updated_at         TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.products (
    product_id      VARCHAR(50) PRIMARY KEY,
    product_name    TEXT NOT NULL,
    category        TEXT NOT NULL,
    subcategory     TEXT,
    brand           TEXT,
    cost_price      NUMERIC(10, 2),
    selling_price   NUMERIC(10, 2),
    margin_pct      NUMERIC(5, 2) GENERATED ALWAYS AS (
                        CASE WHEN selling_price > 0
                             THEN ROUND(((selling_price - cost_price) / selling_price) * 100, 2)
                             ELSE 0
                        END
                    ) STORED,
    stock_quantity  INTEGER,
    sku             TEXT,
    _updated_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id        VARCHAR(50) PRIMARY KEY,
    customer_id     VARCHAR(50) REFERENCES silver.customers(customer_id),
    order_date      DATE NOT NULL,
    ship_date       DATE,
    days_to_ship    INTEGER GENERATED ALWAYS AS (
                        CASE WHEN ship_date IS NOT NULL
                             THEN (ship_date - order_date)
                             ELSE NULL
                        END
                    ) STORED,
    shipping_method TEXT,
    order_status    VARCHAR(30),
    total_amount    NUMERIC(12, 2),
    discount_code   TEXT,
    _updated_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.order_items (
    item_id         VARCHAR(50) PRIMARY KEY,
    order_id        VARCHAR(50) REFERENCES silver.orders(order_id),
    product_id      VARCHAR(50) REFERENCES silver.products(product_id),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10, 2) NOT NULL,
    discount_pct    NUMERIC(5, 2) DEFAULT 0,
    line_total      NUMERIC(12, 2) GENERATED ALWAYS AS (
                        ROUND(quantity * unit_price * (1 - discount_pct / 100), 2)
                    ) STORED,
    _updated_at     TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- GOLD LAYER — Analytics-ready (managed by dbt)
-- ─────────────────────────────────────────────────────────────────────────────

-- Note: Gold tables are created and managed by dbt.
-- Schemas are pre-created here for permission setup.

GRANT USAGE ON SCHEMA bronze TO PUBLIC;
GRANT USAGE ON SCHEMA silver TO PUBLIC;
GRANT USAGE ON SCHEMA gold   TO PUBLIC;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold   TO PUBLIC;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA silver TO PUBLIC;

-- ─────────────────────────────────────────────────────────────────────────────
-- INDEXES for query performance
-- ─────────────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_orders_customer    ON silver.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date        ON silver.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status      ON silver.orders(order_status);
CREATE INDEX IF NOT EXISTS idx_items_order        ON silver.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_items_product      ON silver.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_products_category  ON silver.products(category);
CREATE INDEX IF NOT EXISTS idx_customers_segment  ON silver.customers(customer_segment);
CREATE INDEX IF NOT EXISTS idx_customers_reg_date ON silver.customers(registration_date);
