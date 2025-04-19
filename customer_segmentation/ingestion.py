import os
import json
import pandas as pd
from kafka import KafkaConsumer
from pytz import timezone
from datetime import datetime
from sqlalchemy import create_engine, text
from airflow.models import Variable
from loguru import logger

# If you have a config.py with DB_SCHEMA, you can import that too:
from customer_segmentation.config import DB_SCHEMA  # if you like

def get_db_engine():
    """
    Returns a SQLAlchemy engine for the Postgres database
    using environment variables for credentials.
    """
    DB_HOST = os.getenv("DB_HOST", "postgres")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_USER = os.getenv("DB_USER", "airflow")
    DB_PASS = os.getenv("DB_PASS", "airflow")
    DB_NAME = os.getenv("DB_NAME", "airflow")

    conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str)
    return engine

def consume_and_store(topic, bootstrap_servers, group_id="bridgecart_consumer_group", timeout_ms=5000):
    """
    Consumes messages from Kafka and inserts them into 'raw_customers'
    in the 'bridgecart_customer_data' schema (if DB_SCHEMA is used).
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        group_id=group_id,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest"
    )

    records = []
    for message in consumer:
        records.append(message.value)

    if not records:
        logger.info("No new messages found in Kafka. Nothing to load.")
        return

    df = pd.DataFrame(records)
    logger.info(f"Kafka consumer read {len(df)} new messages from topic '{topic}'.")

    # Add IST timestamp for auditing
    ist = timezone("Asia/Kolkata")
    df["inserted_at"] = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    engine = get_db_engine()
    # If you're using a specific schema, pass schema=DB_SCHEMA below:
    df.to_sql(
        name="raw_customers",
        con=engine,
        if_exists="append",
        index=False,
        schema=DB_SCHEMA  # <-- or remove if you're using public schema
    )
    logger.info(f"Inserted {len(df)} records into {DB_SCHEMA}.raw_customers table.")

    # Manually commit offsets so we don't re-read these messages next time
    consumer.commit()
    logger.info("Offsets committed.")
