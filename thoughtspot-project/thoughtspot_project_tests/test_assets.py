import os
import pandas as pd
import sqlite3
import pytest
from dagster import Failure, build_op_context
from thoughtspot_project.assets import sales_data_partitioned, customer_data, customer_aggregates

# Test database path
TEST_DATABASE_PATH = "data/thoughtspot.db"
TEST_SALES_CSV = "test_data/transactions.csv"
TEST_CUSTOMER_CSV = "test_data/customer.csv"

@pytest.fixture(scope="module")
def setup_test_database():
    os.makedirs(os.path.dirname(TEST_DATABASE_PATH), exist_ok=True)
    yield
    os.remove(TEST_DATABASE_PATH)

@pytest.fixture
def mock_sales_data():
    df = pd.DataFrame({
        "TRANSACTION_ID": [1, 2, 3],
        "CUSTOMER_ID": [1001, 1002, 1001],
        "PRODUCT_ID": [201, 202, 203],
        "QUANTITY": [2, 1, 3],
        "PRICE": [20.0, 35.5, 12.0],
        "TRANSACTION_DATE": pd.to_datetime(["2025-02-24 10:00", "2025-02-24 11:00", "2025-02-24 10:30"])
    })
    df.to_csv(TEST_SALES_CSV, index=False)
    return df

@pytest.fixture
def mock_customer_data():
    df = pd.DataFrame({
        "CUSTOMER_ID": [1001, 1002, 1003],
        "NAME": ["Alice", "Bob", "Charlie"],
        "AGE": [25, 30, 35],
        "COUNTRY": ["USA", "Canada", "UK"]
    })
    df.to_csv(TEST_CUSTOMER_CSV, index=False)
    return df

# Test for sales_data_partitioned
def test_sales_data_partitioned():
    context = build_op_context(partition_key="2025-02-24-10:00")
    df = sales_data_partitioned(context)
    assert not df.empty, "Sales data partitioning failed."

    with sqlite3.connect(TEST_DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
    assert any("sales_table_" in t for t in tables), "Partitioned table not created."

# Test for customer_data
def test_customer_data():
    df = customer_data()
    assert not df.empty, "Customer data loading failed."

    with sqlite3.connect(TEST_DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='customer_data'")
        assert cursor.fetchone(), "Customer data table not created."

# Test for customer_aggregates
def test_customer_aggregates():
    df = customer_aggregates(customer_data())
    assert not df.empty, "Customer aggregates computation failed."
    
    with sqlite3.connect(TEST_DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='customer_aggregated_data'")
        assert cursor.fetchone(), "Aggregated data table not created."
