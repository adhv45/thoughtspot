import os
import pandas as pd
import sqlite3
import numpy as np
from dagster import asset, HourlyPartitionsDefinition, Failure, get_dagster_logger, AssetExecutionContext

logger = get_dagster_logger()

DATABASE_PATH = "data/thoughtspot.db"
SALES_CSV_DATA = "data/transactions.csv"
CUSTOMER_CSV_PATH = "data/customer.csv"

# Initialize start date dynamically
try:
    df = pd.read_csv(SALES_CSV_DATA, parse_dates=["TRANSACTION_DATE"], encoding="ISO-8859-1")
    df["TRANSACTION_DATE"] = pd.to_datetime(df["TRANSACTION_DATE"], format="%Y-%m-%d %H:%M")
    start_date = df["TRANSACTION_DATE"].min()
except FileNotFoundError:
    logger.error(f"File {SALES_CSV_DATA} not found. Please check the path.")
    raise Failure(f"File {SALES_CSV_DATA} not found.")
except Exception as e:
    logger.error(f"Error reading {SALES_CSV_DATA}: {str(e)}")
    raise Failure(f"Error reading {SALES_CSV_DATA}: {str(e)}")

# Define hourly partitioning
hourly_partitions = HourlyPartitionsDefinition(start_date=start_date)

@asset(partitions_def=hourly_partitions)
def sales_data_partitioned(context: AssetExecutionContext) -> pd.DataFrame:
    """Loads, partitions, and stores sales data by hour with logging."""
    try:
        logger.info("Starting sales data processing.")
        partition_hour_str = context.partition_key  # Dagster provides the partition key
        partition_hour = pd.to_datetime(partition_hour_str, format="%Y-%m-%d-%H:%M")

        logger.info(f"Processing sales data for partition: {partition_hour}")

        # Ensure database directory exists
        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)

        # Read data
        logger.info(f"Reading data from {SALES_CSV_DATA}.")
        df = pd.read_csv(SALES_CSV_DATA, parse_dates=["TRANSACTION_DATE"], encoding="ISO-8859-1")
        df["TRANSACTION_DATE"] = pd.to_datetime(df["TRANSACTION_DATE"], format="%Y-%m-%d %H:%M")

        # Get partition key (hourly)
        # partition_hour = pd.to_datetime(hourly_partitions.get_partition_keys()[-1], format="%Y-%m-%d-%H:%M")

        # Filter data for the partition
        df_partitioned = df[df["TRANSACTION_DATE"].dt.floor("h") == partition_hour]

        if df_partitioned.empty:
            logger.warning(f"No data available for partition: {partition_hour}")
            return pd.DataFrame()  # Ensure return type is a DataFrame

        # Store partitioned data in SQLite
        partition_table = f"sales_table_{partition_hour.strftime('%Y%m%d%H')}"
        logger.info(f"Storing data in table: {partition_table}")

        with sqlite3.connect(DATABASE_PATH) as conn:
            df_partitioned.to_sql(partition_table, conn, if_exists="replace", index=False)

        logger.info(f"Successfully stored {len(df_partitioned)} records in {partition_table}.")

        logger.info(f"Returning DataFrame with shape: {df_partitioned.shape}")  # Debugging
        return df  # Ensure the function returns a DataFrame

    except FileNotFoundError:
        logger.error(f"File {SALES_CSV_DATA} not found.")
        raise Failure(f"File {SALES_CSV_DATA} not found. Ensure the data exists.")
    except sqlite3.Error as db_err:
        logger.error(f"Database error: {str(db_err)}")
        raise Failure(f"Database error while inserting partitioned data: {str(db_err)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise Failure(f"Unexpected error during partitioning: {str(e)}")

@asset
def customer_data() -> pd.DataFrame:
    """Loads, and stores customer data in SQLite."""
    try:
        logger.info("Starting customer data processing.")

        # Ensure database directory exists
        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)

        # Read customer data
        logger.info(f"Reading data from {CUSTOMER_CSV_PATH}.")
        df = pd.read_csv(CUSTOMER_CSV_PATH, encoding="ISO-8859-1")

        # Store data in SQLite
        logger.info("Storing customer data into SQLite.")
        with sqlite3.connect(DATABASE_PATH) as conn:
            df.to_sql("customer_data", conn, if_exists="replace", index=False)

        logger.info(f"Successfully stored {len(df)} records in 'customer_data' table.")
        return df

    except FileNotFoundError:
        logger.error(f"File {CUSTOMER_CSV_PATH} not found.")
        raise Failure(f"File {CUSTOMER_CSV_PATH} not found. Ensure the data exists.")
    except sqlite3.Error as db_err:
        logger.error(f"Database error: {str(db_err)}")
        raise Failure(f"Database error while inserting customer data: {str(db_err)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise Failure(f"Unexpected error during customer data processing: {str(e)}")

@asset(deps=[sales_data_partitioned, customer_data])
def customer_aggregates(customer_data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates transaction data to compute:
    - Total unique products purchased by each customer
    - Sum of quantities purchased by each customer
    - Sum of price paid by each customer
    """
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            # Step 1: Retrieve all partitioned sales tables
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'sales_table_%'")
            partition_tables = [row[0] for row in cursor.fetchall()]

            if not partition_tables:
                raise Failure("No partitioned sales data found.")

            # Step 2: Load all partitions
            sales_partitions = [pd.read_sql(f"SELECT * FROM {table}", conn) for table in partition_tables]
            sales_data = pd.concat(sales_partitions, ignore_index=True) if sales_partitions else pd.DataFrame()

        # Aggregation
        aggregated_df = sales_data.groupby("CUSTOMER_ID").agg(
            TOTAL_UNIQUE_PRODUCTS=pd.NamedAgg(column="PRODUCT_ID", aggfunc=lambda x: x.nunique()),
            TOTAL_QUANTITY=pd.NamedAgg(column="QUANTITY", aggfunc="sum"),
            TOTAL_SPENT=pd.NamedAgg(column="PRICE", aggfunc="sum"),
        ).reset_index()
        
        df_joined = customer_data.merge(aggregated_df, on="CUSTOMER_ID", how="left")

        # Store data in SQLite
        logger.info("Storing customer data into SQLite.")
        with sqlite3.connect(DATABASE_PATH) as conn:
            df_joined.to_sql("customer_aggregated_data", conn, if_exists="replace", index=False)

        logger.info(f"Successfully stored {len(df_joined)} records in 'customer_aggregated_data' table.")
        return df_joined

    except Exception as e:
        raise Failure(f"Error processing customer aggregates: {str(e)}")
