import pandas as pd
from pytz import timezone
from datetime import datetime
from sqlalchemy import text
from airflow.models import Variable
from sklearn.cluster import KMeans
from loguru import logger

from customer_segmentation.modeling.predict import predict_segments, predict_rowwise
from customer_segmentation.ingestion import get_db_engine
from customer_segmentation.config import DB_SCHEMA


def segment_and_load():
    engine = get_db_engine()

    # 1. Fetch only new (incremental) rows from silver_customers (using last_silver_ts)
    last_silver_ts = Variable.get("last_silver_ts", default_var="1970-01-01 00:00:00")
    query = text(f"""
        SELECT
            customer_id,
            name,
            age,
            income,
            gender,
            mobile,
            purchase_date,
            purchase_amount,
            coupon_used,
            discount_amount,
            payment_method,
            product_category,
            inserted_at
        FROM {DB_SCHEMA}.silver_customers
        WHERE inserted_at > :last_ts
    """)

    df = pd.read_sql(query, engine, params={"last_ts": last_silver_ts})
    if df.empty:
        logger.info(f"No new silver data since last_silver_ts={last_silver_ts}. Nothing to process.")
        return

    logger.info(f"Fetched {len(df)} new silver records (inserted_at > {last_silver_ts}).")

    # 2. Load all silver data for building "full" references (e.g., training KMeans, computing total CLV)
    full_silver_df = pd.read_sql(f"SELECT * FROM {DB_SCHEMA}.silver_customers", engine)

    # 3. Compute CLV from the full dataset
    clv_df = (
        full_silver_df
        .groupby("customer_id")["purchase_amount"]
        .sum()
        .reset_index(name="clv")
    )

    # Merge CLV into just the new (incremental) records
    df = df.merge(clv_df, on="customer_id", how="left")

    # Simple churn logic (assuming everyone's left today lol)
    df["today"] = pd.Timestamp.now()
    df["days_since_purchase"] = (df["today"] - df["purchase_date"]).dt.days
    df["churn_flag"] = df["days_since_purchase"].apply(lambda x: 1 if x > 14 else 0)

    # 4. Train or load a KMeans model on the full dataset's aggregated features
    #    (For real usage, you would typically load a pre-trained model from file.)
    clv_for_seg = (
        full_silver_df
        .groupby("customer_id")
        .agg({"age": "last", "income": "last", "purchase_amount": "sum"})
        .reset_index()
        .rename(columns={"purchase_amount": "clv"})
    )

    seg_data = clv_for_seg[["age", "income", "clv"]].dropna()
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    kmeans.fit(seg_data)

    # 5. Merge the incremental DataFrame with the training reference for CLV, then predict segments
    #    (We match on customer_id, age, income for consistency with how the code was set up)
    df = df.merge(clv_for_seg, on=["customer_id", "age", "income"], how="left", suffixes=("", "_seg"))
    df.drop(columns=["clv"], errors="ignore", inplace=True)
    df.rename(columns={"clv_seg": "clv"}, inplace=True)

    df["segment"] = predict_segments(kmeans, df)

    # 6. Also compute metrics on the entire silver dataset (for aggregated KPI reporting)
    full_silver_df = full_silver_df.merge(clv_for_seg, on="customer_id", how="left", suffixes=("", "_seg"))
    full_silver_df.rename(columns={"clv_seg": "clv"}, inplace=True)

    full_silver_df["days_since_purchase"] = (pd.Timestamp.now() - full_silver_df["purchase_date"]).dt.days
    full_silver_df["churn_flag"] = full_silver_df["days_since_purchase"].apply(lambda x: 1 if x > 14 else 0)
    full_silver_df["segment"] = predict_segments(kmeans, full_silver_df)

    # 7. Calculate segment metrics for SCD2
    seg_metrics = []
    for seg_id in sorted(full_silver_df["segment"].dropna().unique()):
        seg_subset = full_silver_df[full_silver_df["segment"] == seg_id]
        avg_clv = seg_subset["clv"].mean()
        churn_rate = seg_subset["churn_flag"].mean()
        aov = seg_subset["purchase_amount"].mean()
        total_sales = full_silver_df["purchase_amount"].sum()
        seg_sales = seg_subset["purchase_amount"].sum()
        segment_contribution = seg_sales / total_sales if total_sales else 0
        seg_metrics.append({
            "segment": int(seg_id),
            "avg_clv": round(avg_clv, 2) if avg_clv else 0,
            "churn_rate": round(churn_rate, 4),
            "aov": round(aov, 2) if aov else 0,
            "segment_contribution": round(segment_contribution, 4)
        })
    metrics_df = pd.DataFrame(seg_metrics)

    # 8. Insert new incremental rows into gold_customers_segments
    ist = timezone("Asia/Kolkata")
    now_ist = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    df["inserted_at"] = now_ist

    # Choose the columns that match gold_customers_segments (including coupon, discount, payment, category)
    df_final = df[
        [
            "customer_id",
            "name",
            "age",
            "income",
            "gender",
            "purchase_date",
            "purchase_amount",
            "coupon_used",
            "discount_amount",
            "payment_method",
            "product_category",
            "clv",
            "churn_flag",
            "segment",
            "inserted_at"
        ]
    ]

    df_final.to_sql(
        "gold_customers_segments",
        engine,
        if_exists="append",
        index=False,
        schema=DB_SCHEMA
    )
    logger.info(f"Inserted {len(df_final)} incremental rows into {DB_SCHEMA}.gold_customers_segments table.")

    # 9. SCD2 Upsert for gold_segment_metrics_scd2
    for row in metrics_df.to_dict(orient="records"):
        seg = row["segment"]
        avg_clv = row["avg_clv"]
        churn_rate = row["churn_rate"]
        aov = row["aov"]
        segment_contribution = row["segment_contribution"]

        check_sql = text(f"""
            SELECT id, avg_clv, churn_rate, aov, segment_contribution
            FROM {DB_SCHEMA}.gold_segment_metrics_scd2
            WHERE segment = :seg
              AND scd_is_current = TRUE
            ORDER BY id DESC
            LIMIT 1
        """)
        with engine.begin() as conn:
            current_rec = conn.execute(check_sql, {"seg": seg}).fetchone()
            if not current_rec:
                # No active record for this segment => insert a new one
                insert_sql = text(f"""
                    INSERT INTO {DB_SCHEMA}.gold_segment_metrics_scd2 (
                        segment, avg_clv, churn_rate, aov, segment_contribution,
                        scd_start_date, scd_end_date, scd_is_current
                    )
                    VALUES (
                        :segment, :avg_clv, :churn_rate, :aov, :segment_contribution,
                        :start_date, NULL, TRUE
                    )
                """)
                conn.execute(insert_sql, {
                    "segment": seg,
                    "avg_clv": avg_clv,
                    "churn_rate": churn_rate,
                    "aov": aov,
                    "segment_contribution": segment_contribution,
                    "start_date": now_ist,
                })
            else:
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
                    # Close out old record
                    close_sql = text(f"""
                        UPDATE {DB_SCHEMA}.gold_segment_metrics_scd2
                        SET scd_end_date = :end_date, scd_is_current = FALSE
                        WHERE id = :rec_id
                    """)
                    conn.execute(close_sql, {"end_date": now_ist, "rec_id": current_rec["id"]})

                    # Insert new record as current
                    insert_sql = text(f"""
                        INSERT INTO {DB_SCHEMA}.gold_segment_metrics_scd2 (
                            segment, avg_clv, churn_rate, aov, segment_contribution,
                            scd_start_date, scd_end_date, scd_is_current
                        )
                        VALUES (
                            :segment, :avg_clv, :churn_rate, :aov, :segment_contribution,
                            :start_date, NULL, TRUE
                        )
                    """)
                    conn.execute(insert_sql, {
                        "segment": seg,
                        "avg_clv": avg_clv,
                        "churn_rate": churn_rate,
                        "aov": aov,
                        "segment_contribution": segment_contribution,
                        "start_date": now_ist,
                    })

    logger.info("SCD2 Upsert completed for gold_segment_metrics_scd2.")

    # 10. Update the Airflow Variable so next run only picks up new data
    new_max_silver_ts = df["inserted_at"].max()
    Variable.set("last_silver_ts", new_max_silver_ts)
    logger.info(f"Set last_silver_ts Airflow Variable to {new_max_silver_ts}")
