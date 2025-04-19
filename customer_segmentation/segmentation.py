import pandas as pd
from pytz import timezone
from datetime import datetime
from sqlalchemy import text
from airflow.models import Variable
from sklearn.cluster import KMeans
from loguru import logger

from customer_segmentation.ingestion import get_db_engine
from customer_segmentation.config import DB_SCHEMA

def segment_and_load():
    engine = get_db_engine()

    last_silver_ts = Variable.get("last_silver_ts", default_var="1970-01-01 00:00:00")
    query = text("""
        SELECT
            customer_id, name, age, income, gender, mobile,
            purchase_date, purchase_amount, inserted_at
        FROM bridgecart_customer_data.silver_customers
        WHERE inserted_at > :last_ts
    """)

    df = pd.read_sql(query, engine, params={"last_ts": last_silver_ts})
    if df.empty:
        logger.info(f"No new silver data since last_silver_ts={last_silver_ts}. Nothing to process.")
        return

    logger.info(f"Fetched {len(df)} new silver records (inserted_at > {last_silver_ts}).")

    full_silver_df = pd.read_sql("SELECT * FROM bridgecart_customer_data.silver_customers", engine)
    clv_df = full_silver_df.groupby("customer_id")["purchase_amount"].sum().reset_index(name="clv")
    df = df.merge(clv_df, on="customer_id", how="left")

    df["today"] = pd.Timestamp.now()
    df["days_since_purchase"] = (df["today"] - df["purchase_date"]).dt.days
    df["churn_flag"] = df["days_since_purchase"].apply(lambda x: 1 if x > 14 else 0)

    clv_for_seg = full_silver_df.groupby("customer_id").agg({
        "age": "last",
        "income": "last",
        "purchase_amount": "sum"
    }).reset_index()
    clv_for_seg.rename(columns={"purchase_amount": "clv"}, inplace=True)
    seg_data = clv_for_seg[["age", "income", "clv"]].dropna()

    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    kmeans.fit(seg_data)

    df = df.merge(clv_for_seg, on=["customer_id", "age", "income"], how="left", suffixes=("", "_seg"))
    df.drop(columns=["clv"], errors="ignore", inplace=True)
    df.rename(columns={"clv_seg": "clv"}, inplace=True)

    df["segment"] = kmeans.predict(df[["age", "income", "clv"]].fillna(0))

    full_silver_df = full_silver_df.merge(clv_for_seg, on=["customer_id"], how="left", suffixes=("", "_seg"))
    full_silver_df.rename(columns={"clv_seg": "clv"}, inplace=True)
    full_silver_df["days_since_purchase"] = (pd.Timestamp.now() - full_silver_df["purchase_date"]).dt.days
    full_silver_df["churn_flag"] = full_silver_df["days_since_purchase"].apply(lambda x: 1 if x > 14 else 0)

    def assign_segment(row):
        if pd.isnull(row["age"]) or pd.isnull(row["income"]) or pd.isnull(row["clv"]):
            return None
        return kmeans.predict([[row["age"], row["income"], row["clv"]]])[0]

    full_silver_df["segment"] = full_silver_df.apply(assign_segment, axis=1)

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

    from pytz import timezone
    ist = timezone("Asia/Kolkata")
    now_ist = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    df["inserted_at"] = now_ist

    df_final = df[
        [
            "customer_id", "name", "age", "income", "gender",
            "purchase_date", "purchase_amount", "clv",
            "churn_flag", "segment", "inserted_at"
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

    for row in metrics_df.to_dict(orient="records"):
        seg = row["segment"]
        avg_clv = row["avg_clv"]
        churn_rate = row["churn_rate"]
        aov = row["aov"]
        segment_contribution = row["segment_contribution"]

        check_sql = text("""
            SELECT id, avg_clv, churn_rate, aov, segment_contribution
            FROM bridgecart_customer_data.gold_segment_metrics_scd2
            WHERE segment = :seg
              AND scd_is_current = TRUE
            ORDER BY id DESC
            LIMIT 1
        """)
        with engine.begin() as conn:
            current_rec = conn.execute(check_sql, {"seg": seg}).fetchone()
            if not current_rec:
                insert_sql = text("""
                    INSERT INTO bridgecart_customer_data.gold_segment_metrics_scd2 (
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
                    close_sql = text("""
                        UPDATE bridgecart_customer_data.gold_segment_metrics_scd2
                        SET scd_end_date = :end_date, scd_is_current = FALSE
                        WHERE id = :rec_id
                    """)
                    conn.execute(close_sql, {"end_date": now_ist, "rec_id": current_rec["id"]})

                    insert_sql = text("""
                        INSERT INTO bridgecart_customer_data.gold_segment_metrics_scd2 (
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

    new_max_silver_ts = df["inserted_at"].max()
    Variable.set("last_silver_ts", new_max_silver_ts)
    logger.info(f"Set last_silver_ts Airflow Variable to {new_max_silver_ts}")
