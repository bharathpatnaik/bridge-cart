"""
Airflow DAG to:
1. Produce synthetic data with Faker into Kafka (configurable 'n' records).
2. Consume from Kafka => land into raw layer.
3. Clean/transform => silver.
4. Segment => gold, store in final reporting table or file.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import os

##############################
# Environment / Config
##############################
N_RECORDS = int(os.getenv("FAKER_RECORDS", "1000"))
KAFKA_TOPIC = "bridgecart_customers"
BOOTSTRAP_SERVERS = "kafka:9092"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


##############################
# 1. Produce Data to Kafka
##############################
def produce_data_to_kafka(**context):
    """
    Generates N_RECORDS of synthetic data and pushes them to a Kafka topic.
    """
    from faker import Faker
    import json
    from kafka import KafkaProducer
    import random

    fake = Faker()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # produce the data
    for _ in range(N_RECORDS):
        customer_id = fake.uuid4()
        name = fake.name()
        age = random.randint(18, 75)
        income = random.randint(20000, 150000)
        gender = random.choice(["M", "F"])
        mobile = fake.phone_number()
        purchase_date = fake.date_time_between(start_date="-30d", end_date="now").isoformat()
        purchase_amount = round(random.uniform(5.0, 1000.0), 2)

        record = {
            "customer_id": customer_id,
            "name": name,
            "age": age,
            "income": income,
            "gender": gender,
            "mobile": mobile,
            "purchase_date": purchase_date,
            "purchase_amount": purchase_amount,
        }

        producer.send(KAFKA_TOPIC, record)

    producer.flush()
    producer.close()
    print(f"Produced {N_RECORDS} records to Kafka topic {KAFKA_TOPIC}")


##############################
# 2. Consume Data from Kafka
##############################
def consume_data_from_kafka(**context):
    """
    Consumes data from Kafka and writes it to the raw layer in CSV (or Parquet).
    We keep track of last read offset for incremental ingestion if desired.
    """
    import json
    from kafka import KafkaConsumer
    import pandas as pd
    import os

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="bridgecart_consumer_group",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    records = []
    for message in consumer:
        data = message.value
        records.append(data)

    df = pd.DataFrame(records)
    raw_dir = os.path.join(os.getcwd(), "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    raw_file = os.path.join(raw_dir, "customers_raw.csv")
    df.to_csv(raw_file, index=False)
    print(f"Consumed {len(df)} records from Kafka into {raw_file}")


##############################
# 3. Transform / Clean => Silver
##############################
def transform_and_clean(**context):
    """
    Reads raw data from CSV => cleans => writes to 'interim' or 'silver'.
    - handle missing values
    - remove duplicates
    - fix date formats
    """
    import pandas as pd
    import os

    raw_file = os.path.join(os.getcwd(), "data", "raw", "customers_raw.csv")
    df = pd.read_csv(raw_file)

    # Basic cleaning
    df.drop_duplicates(subset=["customer_id", "purchase_date"], inplace=True)
    # Missing ages => median
    df["age"] = df["age"].fillna(df["age"].median())
    # Missing purchase_amount => 0
    df["purchase_amount"] = df["purchase_amount"].fillna(0)
    # Convert purchase_date
    df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")

    # Additional transformations (e.g. standardizing gender?)
    df["gender"] = df["gender"].map({"M": 0, "F": 1})

    # Write out
    interim_dir = os.path.join(os.getcwd(), "data", "interim")
    os.makedirs(interim_dir, exist_ok=True)
    silver_file = os.path.join(interim_dir, "customers_silver.csv")
    df.to_csv(silver_file, index=False)
    print(f"Transformed/cleaned data to {silver_file}")


##############################
# 4. Segmentation => Gold
##############################
def segment_and_load(**context):
    """
    Reads silver => calculates CLV, does K-means => writes final gold data.
    Also calculates 4 main KPIs by segment:
    1) Customer Lifetime Value Growth by Segment
    2) Customer Churn Rate by Segment
    3) Average Order Value (AOV) by Segment
    4) Segment Contribution to Total Sales
    """
    import pandas as pd
    import os
    from sklearn.cluster import KMeans

    silver_file = os.path.join(os.getcwd(), "data", "interim", "customers_silver.csv")
    df = pd.read_csv(silver_file, parse_dates=["purchase_date"])

    # Example "CLV" calculation: sum of purchase_amount for each customer
    # (In real life, you'd consider discount rates, purchase frequency, etc.)
    clv_df = df.groupby("customer_id")["purchase_amount"].sum().reset_index(name="CLV")
    # Merge it back
    df = df.merge(clv_df, on="customer_id", how="left")

    # We'll do a quick group-based approach to churn:
    # Here, let's define "churn" if last purchase > 14 days from "today" for demonstration
    # This is an oversimplification for a quick example.
    df["today"] = pd.Timestamp.now()
    df["days_since_purchase"] = (df["today"] - df["purchase_date"]).dt.days
    df["churn_flag"] = df["days_since_purchase"].apply(lambda x: 1 if x > 14 else 0)

    # K-means segmentation on [age, income, CLV]
    # Prepare data
    seg_data = df[["age", "income", "CLV"]].dropna()
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    kmeans.fit(seg_data)
    df["segment"] = kmeans.predict(df[["age", "income", "CLV"]].fillna(0))

    # Compute KPIs by segment:
    # 1) Customer Lifetime Value Growth by Segment => we'll do an average CLV by segment
    segment_clv = df.groupby("segment")["CLV"].mean().reset_index(name="avg_CLV")
    # 2) Customer Churn Rate by Segment => churn_flag average
    segment_churn = df.groupby("segment")["churn_flag"].mean().reset_index(name="churn_rate")
    # 3) Average Order Value (AOV) by Segment => average purchase_amount
    segment_aov = df.groupby("segment")["purchase_amount"].mean().reset_index(name="AOV")
    # 4) Segment Contribution to Total Sales => sum purchase_amount / total
    total_sales = df["purchase_amount"].sum()
    seg_sales = df.groupby("segment")["purchase_amount"].sum().reset_index(name="segment_sales")
    seg_sales["segment_contribution"] = seg_sales["segment_sales"] / total_sales

    # Merge these metrics into a single table
    metrics = segment_clv.merge(segment_churn, on="segment")
    metrics = metrics.merge(segment_aov, on="segment")
    metrics = metrics.merge(seg_sales[["segment", "segment_contribution"]], on="segment")

    # Write final data
    gold_dir = os.path.join(os.getcwd(), "data", "processed")
    os.makedirs(gold_dir, exist_ok=True)
    segmented_file = os.path.join(gold_dir, "customers_gold.csv")
    df.to_csv(segmented_file, index=False)

    # Also store the aggregated metrics
    metrics_file = os.path.join(gold_dir, "segment_metrics.csv")
    metrics.to_csv(metrics_file, index=False)

    print(f"Segmented data => {segmented_file}")
    print(f"Segment metrics => {metrics_file}")

    # (Optional) Load into a Postgres table or other DB here with sqlalchemy if wanted.


##############################
# DAG Definition
##############################
with DAG(
    "customer_segmentation_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    produce_task = PythonOperator(
        task_id="produce_data_to_kafka",
        python_callable=produce_data_to_kafka,
        provide_context=True,
    )

    consume_task = PythonOperator(
        task_id="consume_data_from_kafka",
        python_callable=consume_data_from_kafka,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_and_clean",
        python_callable=transform_and_clean,
        provide_context=True,
    )

    segment_task = PythonOperator(
        task_id="segment_and_load",
        python_callable=segment_and_load,
        provide_context=True,
    )

    produce_task >> consume_task >> transform_task >> segment_task
