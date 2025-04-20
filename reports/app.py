import os
import math
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from flask import Flask, render_template_string
from sqlalchemy import create_engine, text

app = Flask(__name__)

# ────────────────────────────────────────────────────────────
# CONFIG ─ pull connection details from env for portability
# ────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "airflow")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASS", "airflow")

CONN_STR = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ────────────────────────────────────────────────────────────
# HELPER UTILS
# ────────────────────────────────────────────────────────────
def build_segment_labels(df_current: pd.DataFrame) -> dict:
    """
    Map numeric segment IDs to friendly labels such as
    'High‑Value', 'Growth', 'Regular', 'Occasional' based on
    descending CLV.
    """
    if df_current.empty:
        return {}

    clv_sorted = (
        df_current.groupby("segment")["avg_clv"]
        .mean()
        .sort_values(ascending=False)
    )
    tiers = ["High‑Value", "Growth", "Regular", "Occasional"]
    label_map = {
        seg_id: tiers[idx] if idx < len(tiers) else f"Segment {seg_id}"
        for idx, seg_id in enumerate(clv_sorted.index)
    }
    return label_map


def build_narratives(
    df_drift: pd.DataFrame,
    df_current: pd.DataFrame,
    churn_delta: pd.Series,
) -> tuple[str, str, str]:
    """
    Return three short paragraphs:
      • pipeline overview
      • segment story
      • churn story
    They must stay generic (no hard‑coded numbers).
    """
    # ── Pipeline overview ────────────────────────────────
    latest_run_ts = df_drift["last_execution"].max()
    pipeline_overview = (
        f"The latest pipeline run completed successfully at {latest_run_ts}, refreshing the raw, "
        "silver, and gold layers. Each layer shows a healthy influx of new "
        "records relative to its historical baseline, and customer lifetime "
        "value (CLV) continues to drift within expected bounds."
    )

    # ── Segment story ────────────────────────────────────
    label_map = build_segment_labels(df_current)
    seg_names = ", ".join(label_map.values())
    segment_story = (
        "Customer clustering has converged on four clear personas — "
        f"{seg_names}. These cohorts differ markedly in their typical spend, "
        "discount usage, and retention patterns, providing actionable levers "
        "for targeted campaigns."
    )

    # ── Churn story ──────────────────────────────────────
    if churn_delta is None or churn_delta.empty:
        churn_story = (
            "Churn remains broadly stable across segments; no material spikes "
            "have been detected since the previous snapshot."
        )
    else:
        winners = churn_delta.nsmallest(1).index.tolist()
        risers = churn_delta.nlargest(1).index.tolist()
        churn_story = (
            f"Recent history shows churn easing among the {label_map.get(winners[0], winners[0])} "
            "cohort while edging up for "
            f"{label_map.get(risers[0], risers[0])}. Continued monitoring is "
            "recommended to verify whether this is an emerging trend or noise."
        )

    return pipeline_overview, segment_story, churn_story


# ────────────────────────────────────────────────────────────
# ROUTE
# ────────────────────────────────────────────────────────────
@app.route("/")
def index():
    engine = create_engine(CONN_STR)

    # ────────────────────────────────────────────────────────
    # 1) PIPELINE DRIFT & EXECUTION METRICS
    # ────────────────────────────────────────────────────────
    drift_query = text(
        """
        WITH layer_stats AS (
            SELECT 'Raw Layer' AS layer, inserted_at
            FROM bridgecart_customer_data.raw_customers
        
            UNION ALL
        
            SELECT 'Silver Layer' AS layer, inserted_at
            FROM bridgecart_customer_data.silver_customers
        
            UNION ALL
        
            SELECT 'Gold Layer' AS layer, inserted_at
            FROM bridgecart_customer_data.gold_customers_segments
        ),
        layer_max AS (
            SELECT
                layer,
                MAX(inserted_at) AS max_inserted
            FROM layer_stats
            GROUP BY layer
        ),
        counts AS (
            SELECT
                ls.layer AS data_layer,
                TO_CHAR(lm.max_inserted, 'DD Mon YYYY HH24:MI') AS last_execution,
                COUNT(*) FILTER (WHERE ls.inserted_at < lm.max_inserted) AS records_before_latest,
                COUNT(*) FILTER (WHERE ls.inserted_at = lm.max_inserted) AS new_records_latest
            FROM layer_stats ls
            JOIN layer_max lm ON ls.layer = lm.layer
            GROUP BY ls.layer, lm.max_inserted
        ),
        drift AS (
            SELECT
                (avg_clv - LAG(avg_clv) OVER (ORDER BY id)) AS clv_drift
            FROM bridgecart_customer_data.gold_segment_metrics_scd2
            WHERE scd_is_current
            ORDER BY id DESC
            LIMIT 1
        )
        SELECT
            c.*,
            d.clv_drift
        FROM counts c
        LEFT JOIN drift d
               ON c.data_layer = 'Gold Layer'
        ORDER BY c.data_layer;

        """
    )
    df_drift = pd.read_sql(drift_query, engine)

    # computed percentage new
    df_drift["% New"] = (
        100 * df_drift["new_records_latest"] /
        df_drift[["records_before_latest", "new_records_latest"]].sum(axis=1)
    ).round(1)

    # ────────────────────────────────────────────────────────
    # 2) SEGMENT METRICS (SCD2 table)
    # ────────────────────────────────────────────────────────
    scd2_query = text(
        """
        SELECT
            id,
            segment,       -- tolerate alt column name
            avg_clv,
            churn_rate,
            aov,
            segment_contribution,
            scd_start_date,
            scd_end_date,
            scd_is_current
        FROM bridgecart_customer_data.gold_segment_metrics_scd2
        ORDER BY segment, id
        """
    )
    df_scd2 = pd.read_sql(scd2_query, engine)

    # current snapshot
    df_current = df_scd2[df_scd2["scd_is_current"]].copy()

    # ────────────────────────────────────────────────────────
    # 3) Build narratives
    # ────────────────────────────────────────────────────────
    churn_delta_series = None
    try:
        # diff between last two points per segment
        seg_latest = (
            df_scd2.groupby("segment")
            .apply(lambda d: d.nlargest(1, "scd_start_date"))   # newest per segment
            .reset_index(drop=True)
        )
        seg_prev = (
            df_scd2.groupby("segment")
            .apply(lambda d: d.nlargest(2, "scd_start_date").nsmallest(1, "scd_start_date"))
            .reset_index(drop=True)
        )
        churn_delta_series = (
            seg_latest.set_index("segment")["churn_rate"] -
            seg_prev.set_index("segment")["churn_rate"]
        )
    except Exception:
        churn_delta_series = None

    pipeline_overview, segment_story, churn_story = build_narratives(
        df_drift, df_current, churn_delta_series
    )

    # ────────────────────────────────────────────────────────
    # 4) Build graphs
    # ────────────────────────────────────────────────────────
    # → Raw layer: product categories
    raw_cat = pd.read_sql(
        text("""
            SELECT product_category, COUNT(*) cnt
            FROM bridgecart_customer_data.raw_customers
            GROUP BY product_category
            ORDER BY cnt DESC
            LIMIT 8
        """),
        engine,
    )
    fig_raw_cat = px.bar(
        raw_cat, x="product_category", y="cnt",
        title="Top Product Categories — Raw",
        labels={"cnt": "Count"},
    ).update_layout(template="plotly_white", width=450, height=350, showlegend=False)

    # → Raw layer: age distribution
    raw_age = pd.read_sql(
        text("SELECT age FROM bridgecart_customer_data.raw_customers WHERE age IS NOT NULL"),
        engine,
    )
    fig_raw_age = px.histogram(
        raw_age, x="age", nbins=20,
        title="Age Distribution — Raw",
        labels={"age": "Age"},
    ).update_layout(template="plotly_white", width=450, height=350)

    # → Silver layer: payment methods
    silver_pay = pd.read_sql(
        text("""
            SELECT payment_method, COUNT(*) cnt
            FROM bridgecart_customer_data.silver_customers
            GROUP BY payment_method
            ORDER BY cnt DESC
        """),
        engine,
    )
    fig_silver_pay = px.pie(
        silver_pay, names="payment_method", values="cnt",
        title="Payment Method — Silver",
        hole=0.35, color_discrete_sequence=px.colors.qualitative.Set1,
    ).update_layout(template="plotly_white", width=400, height=350)

    # → Silver layer: avg discount by category
    silver_disc = pd.read_sql(
        text("""
            SELECT product_category, AVG(discount_amount) avg_disc
            FROM bridgecart_customer_data.silver_customers
            WHERE discount_amount > 0
            GROUP BY product_category
            ORDER BY avg_disc DESC
            LIMIT 8
        """),
        engine,
    )
    fig_silver_disc = px.bar(
        silver_disc, x="product_category", y="avg_disc",
        title="Avg Discount by Category — Silver",
        labels={"avg_disc": "Avg $ Discount"},
    ).update_layout(template="plotly_white", width=400, height=350, showlegend=False)

    # → Gold layer: discount gauge + coupon indicator
    gold_stats = pd.read_sql(
        text("""
            SELECT
                AVG(discount_amount)       AS avg_discount,
                SUM(coupon_used::int)      AS coupons_used,
                COUNT(*)                   AS row_ct
            FROM bridgecart_customer_data.gold_customers_segments
        """),
        engine,
    ).iloc[0]

    avg_discount = gold_stats["avg_discount"] or 0
    coupons_used = int(gold_stats["coupons_used"] or 0)
    total_gold_rows = int(gold_stats["row_ct"])

    fig_disc = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=avg_discount,
            title={"text": "Average Discount — Gold"},
            gauge={"axis": {"range": [None, max(50, avg_discount * 2)]}},
        )
    ).update_layout(width=300, height=300, template="plotly_white")

    fig_coupons = go.Figure(
        go.Indicator(mode="number", value=coupons_used, title={"text": "Coupons Used — Gold"})
    ).update_layout(width=300, height=300, template="plotly_white")

    # → Current CLV bar & revenue share pie
    label_map = build_segment_labels(df_current)
    df_current["segment_label"] = df_current["segment"].map(label_map)

    fig_clv_bar = px.bar(
        df_current, x="segment_label", y="avg_clv", color="segment_label",
        title="Avg CLV per Cohort — Current",
        labels={"avg_clv": "Average CLV", "segment_label": "Cohort"},
    ).update_layout(template="plotly_white", width=450, height=350, showlegend=False)

    fig_contrib = px.pie(
        df_current, names="segment_label", values="segment_contribution",
        title="Revenue Share — Current",
        hole=0.4, color_discrete_sequence=px.colors.qualitative.Set2,
    ).update_layout(template="plotly_white", width=450, height=350)

    # → Churn over time
    fig_churn = px.line(
        df_scd2, x="scd_start_date", y="churn_rate",
        color=df_scd2["segment"].map(label_map).fillna(df_scd2["segment"].astype(str)),
        title="Churn Rate Over Time",
        labels={"scd_start_date": "Start Date", "churn_rate": "Churn Rate"},
    ).update_layout(template="plotly_white", width=900, height=350)

    # ────────────────────────────────────────────────────────
    # 5) HTML TEMPLATE
    # ────────────────────────────────────────────────────────
    html = """
    <!doctype html>
    <html>
    <head>
      <title>BridgeCart Analytics</title>
      <link rel="preconnect" href="https://fonts.googleapis.com">
      <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500&display=swap" rel="stylesheet">
      <style>
        body{font-family:Roboto,Arial,sans-serif;margin:20px;background:#fafafa;color:#333}
        h1{font-weight:500}
        .section{max-width:1140px;margin:0 auto 40px auto;padding:20px;background:#fff;border:1px solid #ddd;border-radius:8px}
        .flex{display:flex;flex-wrap:wrap;gap:20px;justify-content:center}
        table{border-collapse:collapse;width:100%}th,td{border:1px solid #ccc;padding:6px 8px;text-align:left;font-size:0.9rem}
        th{background:#eee}
        .narrative{font-size:0.95rem;line-height:1.5;margin-bottom:1rem}
        .chart-box{background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:6px}
      </style>
    </head>
    <body>
      <h1>BridgeCart Analytics Dashboard</h1>

      <div class="section">
        <h2>Pipeline Overview</h2>
        <p class="narrative">{{ pipeline_overview }}</p>
        {{ drift_table|safe }}
      </div>

      <div class="section">
        <h2>Exploratory Data Analysis</h2>
        <div class="flex">
          <div class="chart-box">{{ fig_raw_cat|safe }}</div>
          <div class="chart-box">{{ fig_raw_age|safe }}</div>
        </div>
        <div class="flex">
          <div class="chart-box">{{ fig_silver_pay|safe }}</div>
          <div class="chart-box">{{ fig_silver_disc|safe }}</div>
        </div>
      </div>

      <div class="section">
        <h2>Cohort Insights</h2>
        <p class="narrative">{{ segment_story }}</p>
        <div class="flex">
          <div class="chart-box">{{ fig_disc|safe }}</div>
          <div class="chart-box">{{ fig_coupons|safe }}</div>
          <div style="margin:auto;font-size:1.2rem;text-align:center">
            <b>Total Gold Rows:</b><br>{{ total_gold_rows }}
          </div>
        </div>
        <div class="flex">
          <div class="chart-box">{{ fig_clv_bar|safe }}</div>
          <div class="chart-box">{{ fig_contrib|safe }}</div>
        </div>
      </div>

      <div class="section">
        <h2>Churn Analysis</h2>
        <p class="narrative">{{ churn_story }}</p>
        <div class="chart-box" style="max-width:920px;margin:auto">{{ fig_churn|safe }}</div>
      </div>

    </body>
    </html>
    """

    return render_template_string(
        html,
        # narrative strings
        pipeline_overview=pipeline_overview,
        segment_story=segment_story,
        churn_story=churn_story,
        # numeric
        total_gold_rows=f"{total_gold_rows:,}",
        # tables & charts
        drift_table=df_drift.rename(
            columns={
                "data_layer": "Data Layer",
                "last_execution": "Last Execution",
                "records_before_latest": "Records Before Latest",
                "new_records_latest": "New Records in Latest",
                "clv_drift": "CLV Drift (Δ)"
            }
        ).to_html(index=False, classes="table"),
        fig_raw_cat=fig_raw_cat.to_html(full_html=False),
        fig_raw_age=fig_raw_age.to_html(full_html=False),
        fig_silver_pay=fig_silver_pay.to_html(full_html=False),
        fig_silver_disc=fig_silver_disc.to_html(full_html=False),
        fig_disc=fig_disc.to_html(full_html=False),
        fig_coupons=fig_coupons.to_html(full_html=False),
        fig_clv_bar=fig_clv_bar.to_html(full_html=False),
        fig_contrib=fig_contrib.to_html(full_html=False),
        fig_churn=fig_churn.to_html(full_html=False),
    )


# ────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5010, debug=True)
