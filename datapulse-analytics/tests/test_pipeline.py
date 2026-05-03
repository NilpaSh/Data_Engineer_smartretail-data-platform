"""
Unit tests for Bronze loader and Silver transformer.
Run: pytest tests/ -v --cov=ingestion --cov=transformation
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ──────────────────────────────────────────────────────────────────────────────
# Bronze Loader Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestBronzeLoader:
    """Tests for ingestion/bronze_loader.py"""

    def test_table_config_completeness(self):
        """All table configs must have required keys."""
        from ingestion.bronze_loader import TABLE_CONFIG
        required_keys = {"file", "table", "dedup_key", "expected_columns"}
        for name, config in TABLE_CONFIG.items():
            assert required_keys.issubset(config.keys()), \
                f"Table '{name}' missing keys: {required_keys - set(config.keys())}"

    def test_validate_schema_passes_with_expected_columns(self):
        """validate_schema should not raise when all expected columns are present."""
        from ingestion.bronze_loader import validate_schema
        df = pd.DataFrame(columns=["col_a", "col_b", "col_c"])
        validate_schema(df, ["col_a", "col_b", "col_c"], "test")  # Should not raise

    def test_validate_schema_raises_on_missing_columns(self):
        """validate_schema should raise ValueError when columns are missing."""
        from ingestion.bronze_loader import validate_schema
        df = pd.DataFrame(columns=["col_a"])
        with pytest.raises(ValueError, match="Missing expected columns"):
            validate_schema(df, ["col_a", "col_b"], "test")

    def test_validate_schema_allows_extra_columns(self):
        """validate_schema should not raise on extra columns (just warn)."""
        from ingestion.bronze_loader import validate_schema
        df = pd.DataFrame(columns=["col_a", "col_b", "extra_col"])
        validate_schema(df, ["col_a", "col_b"], "test")  # Should not raise

    def test_get_existing_keys_returns_set(self):
        """get_existing_keys should return a set of strings."""
        from ingestion.bronze_loader import get_existing_keys
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine.connect.return_value = mock_conn
        mock_conn.execute.return_value = [("KEY-001",), ("KEY-002",)]

        result = get_existing_keys(mock_engine, "bronze.raw_customers", "customer_id")
        assert isinstance(result, set)
        assert "KEY-001" in result
        assert "KEY-002" in result


# ──────────────────────────────────────────────────────────────────────────────
# Silver Transformer Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestSilverTransformer:
    """Tests for transformation/silver_transformer.py"""

    def test_clean_email_valid(self):
        from transformation.silver_transformer import clean_email
        assert clean_email("user@example.com") == "user@example.com"
        assert clean_email("USER@EXAMPLE.COM") == "user@example.com"  # lowercased

    def test_clean_email_invalid(self):
        from transformation.silver_transformer import clean_email
        assert clean_email("not-an-email") is None
        assert clean_email("") is None
        assert clean_email(None) is None
        assert clean_email("missing@") is None

    def test_clean_numeric_valid(self):
        from transformation.silver_transformer import clean_numeric
        assert clean_numeric("123.45") == pytest.approx(123.45)
        assert clean_numeric("1,234.56") == pytest.approx(1234.56)
        assert clean_numeric(99.99) == pytest.approx(99.99)

    def test_clean_numeric_invalid_returns_default(self):
        from transformation.silver_transformer import clean_numeric
        assert clean_numeric("not_a_number") == 0.0
        assert clean_numeric("not_a_number", default=99.0) == 99.0
        assert clean_numeric(None) == 0.0

    def test_clean_date_valid_formats(self):
        from transformation.silver_transformer import clean_date
        from datetime import date
        assert clean_date("2024-03-15") == date(2024, 3, 15)
        assert clean_date("03/15/2024") == date(2024, 3, 15)

    def test_clean_date_empty(self):
        from transformation.silver_transformer import clean_date
        assert clean_date("") is None
        assert clean_date(None) is None

    def test_clean_date_invalid(self):
        from transformation.silver_transformer import clean_date
        assert clean_date("not-a-date") is None

    def test_standardize_status(self):
        from transformation.silver_transformer import standardize_status
        assert standardize_status("complete") == "completed"
        assert standardize_status("CANCEL") == "cancelled"
        assert standardize_status("shipped") == "shipped"
        assert standardize_status("pending") == "processing"
        assert standardize_status("unknown_status") == "unknown_status"


# ──────────────────────────────────────────────────────────────────────────────
# Data Generation Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestDataGeneration:
    """Tests for data_generation/generate_ecommerce_data.py"""

    def test_generate_customers_count(self):
        from data_generation.generate_ecommerce_data import generate_customers
        customers = generate_customers(50)
        assert len(customers) == 50

    def test_generate_customers_schema(self):
        from data_generation.generate_ecommerce_data import generate_customers
        customers = generate_customers(5)
        required_fields = {
            "customer_id", "first_name", "last_name", "email",
            "city", "state", "country", "customer_segment", "registration_date"
        }
        for c in customers:
            assert required_fields.issubset(set(c.keys()))

    def test_generate_customers_id_format(self):
        from data_generation.generate_ecommerce_data import generate_customers
        customers = generate_customers(5)
        for c in customers:
            assert c["customer_id"].startswith("CUST-")

    def test_generate_products_count(self):
        from data_generation.generate_ecommerce_data import generate_products
        products = generate_products(50)
        assert len(products) == 50

    def test_generate_products_positive_prices(self):
        from data_generation.generate_ecommerce_data import generate_products
        products = generate_products(30)
        for p in products:
            assert float(p["selling_price"]) > 0
            assert float(p["cost_price"]) > 0
            # Cost should not exceed selling price
            assert float(p["cost_price"]) <= float(p["selling_price"])

    def test_generate_orders_count(self):
        from data_generation.generate_ecommerce_data import (
            generate_customers, generate_products, generate_orders_and_items
        )
        customers = generate_customers(20)
        products = generate_products(30)
        orders, items = generate_orders_and_items(customers, products, 50)
        assert len(orders) == 50
        assert len(items) >= 50  # At least 1 item per order

    def test_generate_orders_referential_integrity(self):
        from data_generation.generate_ecommerce_data import (
            generate_customers, generate_products, generate_orders_and_items
        )
        customers = generate_customers(20)
        products = generate_products(30)
        orders, items = generate_orders_and_items(customers, products, 100)

        order_ids = {o["order_id"] for o in orders}
        customer_ids = {c["customer_id"] for c in customers}
        product_ids = {p["product_id"] for p in products}

        for item in items:
            assert item["order_id"] in order_ids
            assert item["product_id"] in product_ids

        for order in orders:
            assert order["customer_id"] in customer_ids
