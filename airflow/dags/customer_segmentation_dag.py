"""
Airflow DAG to:
1. Produce synthetic data with Faker into Kafka.
2. Consume from Kafka => land incrementally into raw table in DB (with IST timestamps).
3. Transform/clean => incrementally load to silver table in DB.
4. Segment => incrementally load gold segments, and manage SCD2 in segment metrics.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os
from customer_segmentation.data_generation import generate_kafka_data


##############################
# Environment / Config
##############################
N_RECORDS = int(Variable.get("FAKER_RECORDS", default_var="1000"))
KAFKA_TOPIC = "bridgecart_customer_pipeline"
BOOTSTRAP_SERVERS = "kafka:9092"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Database credentials from env
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASS", "airflow")
DB_NAME = os.getenv("DB_NAME", "airflow")


def get_db_engine():
    """
    Returns a SQLAlchemy engine for the Postgres database
    using credentials from environment variables.
    """
    from sqlalchemy import create_engine
    conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str)
    return engine


##############################
# 1. Produce Data to Kafka
##############################
def produce_data_to_kafka(**context):
    generate_kafka_data(
        topic=KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        n_records=N_RECORDS
    )


##############################
# 2. Consume Data from Kafka => RAW (Incremental)
##############################
def consume_data_from_kafka(**context):
    """
    Consumes data from Kafka, writes it to the raw table in DB,
    and only processes new messages from the last committed offset.
    We also store inserted_at in IST for auditing.
    """
    import json
    from kafka import KafkaConsumer
    import pandas as pd
    import os
    from pytz import timezone
    from datetime import datetime

    engine = get_db_engine()

    # Setting enable_auto_commit=False so we can manually commit offsets
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        group_id="bridgecart_consumer_group",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
    )

    records = []
    for message in consumer:
        records.append(message.value)

    # If no new records, we can just exit early
    if not records:
        print("No new messages found in Kafka. Nothing to load to RAW.")
        return

    df = pd.DataFrame(records)
    print(f"Kafka consumer read {len(df)} new messages.")

    # Optionally save the data to CSV
    raw_dir = os.path.join(os.getcwd(), "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    raw_file = os.path.join(raw_dir, "customers_raw.csv")
    df.to_csv(raw_file, index=False)
    print(f"Saved raw data to {raw_file}")

    # Add IST timestamp for auditing
    ist = timezone("Asia/Kolkata")
    now_ist = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    df["inserted_at"] = now_ist

    # Insert into raw_customers table
    df.to_sql("raw_customers", engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} records into raw_customers table.")

    # *** Manually commit the offsets here ***
    consumer.commit()
    print("Manually committed Kafka offsets.")

##############################
# 3. Transform/Clean => SILVER (Incremental)
##############################
def transform_and_clean(**context):
    """
    Incrementally read new rows from raw_customers (including new columns),
    clean them, then append them into silver_customers.
    """
    import pandas as pd
    from pytz import timezone
    from datetime import datetime
    from sqlalchemy import text
    from airflow.models import Variable
    import os

    engine = get_db_engine()

    # Last processed timestamp
    last_raw_ts = Variable.get("last_raw_ts", default_var="1970-01-01 00:00:00")

    query = text("""
        SELECT customer_id, name, age, income, gender, mobile,
               purchase_date, purchase_amount, inserted_at,
               coupon_used, discount_amount, payment_method, product_category
        FROM raw_customers
        WHERE inserted_at > :last_ts
    """)

    df = pd.read_sql(query, engine, params={"last_ts": last_raw_ts})

    if df.empty:
        print(f"No new raw data since last_raw_ts={last_raw_ts}. Nothing to process.")
        return

    print(f"Fetched {len(df)} new raw records (inserted_at > {last_raw_ts}).")

    # Basic cleaning
    df.drop_duplicates(subset=["customer_id", "purchase_date"], inplace=True)

    # fill missing ages with median
    df["age"] = df["age"].fillna(df["age"].median())

    # fill missing purchase_amount with 0
    df["purchase_amount"] = df["purchase_amount"].fillna(0)

    # fill missing discount_amount with 0
    df["discount_amount"] = df["discount_amount"].fillna(0)

    # standardize coupon_used => booleans if needed
    df["coupon_used"] = df["coupon_used"].fillna(False)

    # convert purchase_date
    df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")

    # convert gender from 'M'/'F' => 0/1
    df["gender"] = df["gender"].replace({"M": 0, "F": 1})

    # Optional: standardize or clean payment_method, product_category
    # e.g., strip whitespace, set to lower case
    df["payment_method"] = df["payment_method"].str.strip().str.title()  # e.g. "Credit Card"
    df["product_category"] = df["product_category"].str.strip().str.title()

    # Save local CSV if desired
    silver_dir = os.path.join(os.getcwd(), "data", "interim")
    os.makedirs(silver_dir, exist_ok=True)
    silver_file = os.path.join(silver_dir, "customers_silver.csv")
    df.to_csv(silver_file, index=False)
    print(f"Transformed/cleaned data to {silver_file}")

    # Insert into silver_customers
    ist = timezone("Asia/Kolkata")
    now_ist = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    df["inserted_at"] = now_ist
    df.to_sql("silver_customers", engine, if_exists="append", index=False, schema="bridgecart_customer_data")
    print(f"Inserted {len(df)} rows into silver_customers table.")

    # Update the Airflow Variable
    new_max_ts = df["inserted_at"].max()
    Variable.set("last_raw_ts", new_max_ts)
    print(f"Set last_raw_ts Airflow Variable to {new_max_ts}")


##############################
# 4. Segmentation => GOLD (Incremental + SCD2 for Segment Metrics)
##############################
def segment_and_load(**context):
    """
    Reads only new silver data => calculates CLV, K-means => writes incremental
    data into gold_customers_segments. Then for metrics, updates an SCD2 table:
    gold_segment_metrics_scd2.
    """
    import pandas as pd
    import os
    from pytz import timezone
    from datetime import datetime
    from sqlalchemy import text
    from sklearn.cluster import KMeans

    engine = get_db_engine()

    # Get last processed silver timestamp from Airflow Variables (default '1970-01-01')
    last_silver_ts = Variable.get("last_silver_ts", default_var="1970-01-01 00:00:00")

    # Fetch only new silver rows
    query = text(f"""
        SELECT customer_id, name, age, income, gender, mobile,
               purchase_date, purchase_amount, inserted_at
        FROM silver_customers
        WHERE inserted_at > :last_ts
    """)
    df = pd.read_sql(query, engine, params={"last_ts": last_silver_ts})

    if df.empty:
        print(f"No new silver data since last_silver_ts={last_silver_ts}. Nothing to process.")
        return

    print(f"Fetched {len(df)} new silver records (inserted_at > {last_silver_ts}).")

    # Calculate CLV (sum of purchase_amount per customer).
    # But since we only have partial/incremental data, let's do a fresh "full" CLV approach:
    #  => We can fetch the entire silver_customers table to do an accurate CLV.
    # Alternatively, we do an incremental approach with partial aggregates.
    # For demonstration, let's do a quick full approach:
    full_silver_df = pd.read_sql("SELECT * FROM silver_customers", engine)
    # Calculate total CLV from the entire silver dataset, not just the new rows
    clv_df = full_silver_df.groupby("customer_id")["purchase_amount"].sum().reset_index(name="clv")

    # We'll join the new incremental rows with their full CLV.
    df = df.merge(clv_df, on="customer_id", how="left")

    # Quick churn approach: if last purchase > 14 days from "now"
    df["today"] = pd.Timestamp.now()
    df["days_since_purchase"] = (df["today"] - df["purchase_date"]).dt.days
    df["churn_flag"] = df["days_since_purchase"].apply(lambda x: 1 if x > 14 else 0)

    # K-means on [age, income, clv]
    seg_data = full_silver_df[["age", "income", "purchase_amount"]].dropna()
    # Or for consistency with the above clv usage:
    #   we might do a full approach. We'll do a quick approach here:
    clv_for_seg = full_silver_df.groupby("customer_id").agg({
        "age": "last",  # or average
        "income": "last",
        "purchase_amount": "sum"  # total as CLV
    }).reset_index()
    clv_for_seg.rename(columns={"purchase_amount": "clv"}, inplace=True)
    seg_data = clv_for_seg[["age", "income", "clv"]].dropna()

    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    kmeans.fit(seg_data)

    # For the new rows, we predict cluster assignment
    # We merge on (customer_id) with the same clv_for_seg so we can get the same features
    df = df.merge(clv_for_seg, on=["customer_id", "age", "income"], how="left", suffixes=("", "_seg"))
    # Some rows might not merge if "age"/"income" changed over time. This is a simplification.

    df["segment"] = kmeans.predict(df[["age", "income", "clv_seg"]].fillna(0))

    # =============== Compute KPIs across the entire dataset ===============
    # For each segment (in the full updated dataset), we want to get fresh metrics.
    # So let's build a "full gold" view (not just partial).
    # We combine silver + the newly assigned segments to recalc metrics.
    # A simpler approach is to build a single "df_gold" from full_silver_df + cluster assignments for each row
    # but we must do consistent cluster labeling. We'll re-run the KMeans on the entire set for fresh labels.

    # Rebuild a full "silver + clv" DataFrame
    full_silver_df = full_silver_df.merge(clv_df, on="customer_id", how="left")
    # We'll do the same approach for the entire set
    full_silver_df = full_silver_df.merge(
        clv_for_seg[["customer_id", "clv"]],
        on="customer_id",
        how="left",
        suffixes=("", "_ignore"),
    )
    # For simplicity, define a function that assigns segment by merging + predicting
    def assign_segment(row):
        # row has age, income, clv
        if pd.isnull(row["age"]) or pd.isnull(row["income"]) or pd.isnull(row["clv"]):
            return None
        return kmeans.predict([[row["age"], row["income"], row["clv"]]])[0]

    full_silver_df["segment"] = full_silver_df.apply(assign_segment, axis=1)

    # Churn flag (simple)
    full_silver_df["days_since_purchase"] = (pd.Timestamp.now() - full_silver_df["purchase_date"]).dt.days
    full_silver_df["churn_flag"] = full_silver_df["days_since_purchase"].apply(lambda x: 1 if x > 14 else 0)

    # Compute metrics across all data
    seg_metrics = []
    for seg_id in sorted(full_silver_df["segment"].dropna().unique()):
        seg_subset = full_silver_df[full_silver_df["segment"] == seg_id]
        avg_clv = seg_subset["clv"].mean()
        churn_rate = seg_subset["churn_flag"].mean()
        aov = seg_subset["purchase_amount"].mean()
        total_sales = full_silver_df["purchase_amount"].sum()
        seg_sales = seg_subset["purchase_amount"].sum()
        segment_contribution = seg_sales / total_sales if total_sales else 0
        seg_metrics.append(
            {
                "segment": int(seg_id),
                "avg_clv": round(avg_clv, 2) if avg_clv is not None else 0,
                "churn_rate": round(churn_rate, 4),
                "aov": round(aov, 2) if aov is not None else 0,
                "segment_contribution": round(segment_contribution, 4),
            }
        )
    metrics_df = pd.DataFrame(seg_metrics)

    # =============== Write incremental new rows => gold_customers_segments ===============
    # The incremental "df" we built is just the newly processed rows, so let's store them with
    # their newly assigned cluster in gold_customers_segments.
    ist = timezone("Asia/Kolkata")
    now_ist = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    df["inserted_at"] = now_ist

    # Drop extra columns
    df_final = df[
        [
            "customer_id", "name", "age", "income", "gender",
            "purchase_date", "purchase_amount", "clv",
            "churn_flag", "segment", "inserted_at"
        ]
    ]
    df_final.to_sql("gold_customers_segments", engine, if_exists="append", index=False)
    print(f"Inserted {len(df_final)} incremental rows into gold_customers_segments table.")

    # =============== SCD2 logic for gold_segment_metrics_scd2 ===============
    # We'll manually upsert each row to maintain historical changes.
    # For each segment row in metrics_df, we compare to the current active record.
    from sqlalchemy import text

    for row in metrics_df.to_dict(orient="records"):
        seg = row["segment"]
        avg_clv = row["avg_clv"]
        churn_rate = row["churn_rate"]
        aov = row["aov"]
        segment_contribution = row["segment_contribution"]

        # 1. Check if there's an active record for this segment
        check_sql = text("""
            SELECT id, avg_clv, churn_rate, aov, segment_contribution
            FROM gold_segment_metrics_scd2
            WHERE segment = :seg
              AND scd_is_current = TRUE
            ORDER BY id DESC
            LIMIT 1
        """)
        with engine.begin() as conn:
            current_rec = conn.execute(check_sql, {"seg": seg}).fetchone()

            # 2. If no active record, insert a new one (start_date=now, end_date=NULL, is_current=TRUE)
            if not current_rec:
                insert_sql = text("""
                    INSERT INTO gold_segment_metrics_scd2 (
                        segment, avg_clv, churn_rate, aov, segment_contribution,
                        scd_start_date, scd_end_date, scd_is_current
                    )
                    VALUES (
                        :segment, :avg_clv, :churn_rate, :aov, :segment_contribution,
                        :start_date, NULL, TRUE
                    )
                """)
                conn.execute(
                    insert_sql,
                    {
                        "segment": seg,
                        "avg_clv": avg_clv,
                        "churn_rate": churn_rate,
                        "aov": aov,
                        "segment_contribution": segment_contribution,
                        "start_date": now_ist,
                    },
                )
            else:
                # 3. Compare current metrics to the new metrics
                # If they differ, we close out the old record and insert a new one
                old_avg_clv = float(current_rec["avg_clv"])
                old_churn_rate = float(current_rec["churn_rate"])
                old_aov = float(current_rec["aov"])
                old_seg_contrib = float(current_rec["segment_contribution"])

                changed = (
                    abs(old_avg_clv - avg_clv) > 1e-6
                    or abs(old_churn_rate - churn_rate) > 1e-6
                    or abs(old_aov - aov) > 1e-6
                    or abs(old_seg_contrib - segment_contribution) > 1e-6
                )

                if changed:
                    # Close old record
                    close_sql = text("""
                        UPDATE gold_segment_metrics_scd2
                        SET scd_end_date = :end_date, scd_is_current = FALSE
                        WHERE id = :rec_id
                    """)
                    conn.execute(close_sql, {"end_date": now_ist, "rec_id": current_rec["id"]})

                    # Insert new record
                    insert_sql = text("""
                        INSERT INTO gold_segment_metrics_scd2 (
                            segment, avg_clv, churn_rate, aov, segment_contribution,
                            scd_start_date, scd_end_date, scd_is_current
                        )
                        VALUES (
                            :segment, :avg_clv, :churn_rate, :aov, :segment_contribution,
                            :start_date, NULL, TRUE
                        )
                    """)
                    conn.execute(
                        insert_sql,
                        {
                            "segment": seg,
                            "avg_clv": avg_clv,
                            "churn_rate": churn_rate,
                            "aov": aov,
                            "segment_contribution": segment_contribution,
                            "start_date": now_ist,
                        },
                    )
                # If no change, do nothing (keep the old record active)

    print("SCD2 Upsert completed for gold_segment_metrics_scd2.")

    # 6. Update the Airflow Variable for silver to gold
    # We use the new maximum silver inserted_at from the incremental df
    new_max_silver_ts = df["inserted_at"].max()
    Variable.set("last_silver_ts", new_max_silver_ts)
    print(f"Set last_silver_ts Airflow Variable to {new_max_silver_ts}")


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
    )

    consume_task = PythonOperator(
        task_id="consume_data_from_kafka",
        python_callable=consume_data_from_kafka,
    )

    transform_task = PythonOperator(
        task_id="transform_and_clean",
        python_callable=transform_and_clean,
    )

    segment_task = PythonOperator(
        task_id="segment_and_load",
        python_callable=segment_and_load,
    )

    produce_task >> consume_task >> transform_task >> segment_task
