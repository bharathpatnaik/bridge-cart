import os
import pandas as pd
from pytz import timezone
from datetime import datetime
from sqlalchemy import text
from airflow.models import Variable
from loguru import logger

from customer_segmentation.ingestion import get_db_engine
from customer_segmentation.config import DB_SCHEMA

def transform_and_clean():
    """
    Incrementally read new rows from raw_customers,
    clean them, then append them into silver_customers (in DB_SCHEMA).
    """
    engine = get_db_engine()

    last_raw_ts = Variable.get("last_raw_ts", default_var="1970-01-01 00:00:00")
    query = text("""
        SELECT customer_id, name, age, income, gender, mobile,
               purchase_date, purchase_amount, inserted_at,
               coupon_used, discount_amount, payment_method, product_category
        FROM bridgecart_customer_data.raw_customers
        WHERE inserted_at > :last_ts
    """)

    df = pd.read_sql(query, engine, params={"last_ts": last_raw_ts})

    if df.empty:
        logger.info(f"No new raw data since last_raw_ts={last_raw_ts}. Nothing to process.")
        return

    logger.info(f"Fetched {len(df)} new raw records (inserted_at > {last_raw_ts}).")

    # Basic cleaning
    df.drop_duplicates(subset=["customer_id", "purchase_date"], inplace=True)
    df["age"] = df["age"].fillna(df["age"].median())
    df["purchase_amount"] = df["purchase_amount"].fillna(0)
    df["discount_amount"] = df["discount_amount"].fillna(0)
    df["coupon_used"] = df["coupon_used"].fillna(False)
    df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")
    df["gender"] = df["gender"].replace({"M": 0, "F": 1})
    df["payment_method"] = df["payment_method"].str.strip().str.title()
    df["product_category"] = df["product_category"].str.strip().str.title()

    # (Optional) Save local CSV
    from pathlib import Path
    silver_dir = Path(os.getcwd()) / "data" / "interim"
    silver_dir.mkdir(parents=True, exist_ok=True)
    silver_file = silver_dir / "customers_silver.csv"
    df.to_csv(silver_file, index=False)
    logger.info(f"Transformed/cleaned data to {silver_file}")

    # Insert into silver_customers
    ist = timezone("Asia/Kolkata")
    now_ist = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    df["inserted_at"] = now_ist

    df.to_sql(
        "silver_customers",
        engine,
        if_exists="append",
        index=False,
        schema=DB_SCHEMA
    )
    logger.info(f"Inserted {len(df)} rows into {DB_SCHEMA}.silver_customers table.")

    # Update the Airflow Variable
    new_max_ts = df["inserted_at"].max()
    Variable.set("last_raw_ts", new_max_ts)
    logger.info(f"Set last_raw_ts Airflow Variable to {new_max_ts}")
