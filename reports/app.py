import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from flask import Flask, render_template_string
from sqlalchemy import create_engine, text

app = Flask(__name__)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "airflow")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASS", "airflow")

CONN_STR = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@app.route("/")
def index():
    engine = create_engine(CONN_STR)

    # ----------------------------------------------------
    # 1) PIPELINE DRIFT & EXECUTION METRICS
    # ----------------------------------------------------
    drift_query = text(
        """
        WITH raw_info AS (
          SELECT
            to_char(MAX(inserted_at), 'MM/DD FMDay, HH12:MI:SS PM') AS last_execution,
            COUNT(*) FILTER (
              WHERE inserted_at < (SELECT MAX(inserted_at) FROM bridgecart_customer_data.raw_customers)
            ) AS count_before_latest,
            COUNT(*) FILTER (
              WHERE inserted_at = (SELECT MAX(inserted_at) FROM bridgecart_customer_data.raw_customers)
            ) AS count_in_latest_run
          FROM bridgecart_customer_data.raw_customers
        ),
        silver_info AS (
          SELECT
            to_char(MAX(inserted_at), 'MM/DD FMDay, HH12:MI:SS PM') AS last_execution,
            COUNT(*) FILTER (
              WHERE inserted_at < (SELECT MAX(inserted_at) FROM bridgecart_customer_data.silver_customers)
            ) AS count_before_latest,
            COUNT(*) FILTER (
              WHERE inserted_at = (SELECT MAX(inserted_at) FROM bridgecart_customer_data.silver_customers)
            ) AS count_in_latest_run
          FROM bridgecart_customer_data.silver_customers
        ),
        gold_info AS (
          SELECT
            to_char(MAX(inserted_at), 'MM/DD FMDay, HH12:MI:SS PM') AS last_execution,
            COUNT(*) FILTER (
              WHERE inserted_at < (SELECT MAX(inserted_at) FROM bridgecart_customer_data.gold_customers_segments)
            ) AS count_before_latest,
            COUNT(*) FILTER (
              WHERE inserted_at = (SELECT MAX(inserted_at) FROM bridgecart_customer_data.gold_customers_segments)
            ) AS count_in_latest_run
          FROM bridgecart_customer_data.gold_customers_segments
        ),
        drift_info AS (
          SELECT
            (avg_clv - LAG(avg_clv) OVER (ORDER BY id)) AS clv_drift
          FROM bridgecart_customer_data.gold_segment_metrics_scd2
          WHERE scd_is_current = TRUE
          ORDER BY id DESC
          LIMIT 1
        )
        SELECT
          'Raw Layer' AS data_layer,
          r.last_execution,
          r.count_before_latest,
          r.count_in_latest_run,
          NULL::numeric AS clv_drift
        FROM raw_info r

        UNION ALL

        SELECT
          'Silver Layer' AS data_layer,
          s.last_execution,
          s.count_before_latest,
          s.count_in_latest_run,
          NULL::numeric AS clv_drift
        FROM silver_info s

        UNION ALL

        SELECT
          'Gold Layer' AS data_layer,
          g.last_execution,
          g.count_before_latest,
          g.count_in_latest_run,
          d.clv_drift
        FROM gold_info g
        CROSS JOIN drift_info d;
        """
    )
    df_drift = pd.read_sql(drift_query, engine)

    # Rename columns for clarity
    df_drift.rename(
        columns={
            "data_layer": "Data Layer",
            "last_execution": "Last Execution",
            "count_before_latest": "Records Before Latest",
            "count_in_latest_run": "New Records in Latest",
            "clv_drift": "CLV Drift (Δ)"
        },
        inplace=True
    )

    # Generate an HTML table
    drift_table_html = df_drift.to_html(index=False, classes="table table-bordered table-striped")

    # Extract CLV drift for display
    clv_drift_value = df_drift["CLV Drift (Δ)"].dropna().values[0] if not df_drift["CLV Drift (Δ)"].dropna().empty else 0

    # ----------------------------------------------------
    # 2) EDA: RAW LAYER
    # ----------------------------------------------------
    # Bar chart: top product categories
    raw_cat_query = text("""
        SELECT product_category, COUNT(*) as count_cat
        FROM bridgecart_customer_data.raw_customers
        GROUP BY product_category
        ORDER BY count_cat DESC
        LIMIT 8
    """)
    df_raw_cat = pd.read_sql(raw_cat_query, engine)

    fig_raw_cat = px.bar(
        df_raw_cat,
        x="product_category",
        y="count_cat",
        title="Top Product Categories in Raw Layer",
        color="product_category",
        labels={"product_category": "Product Category", "count_cat": "Count"},
    )
    fig_raw_cat.update_layout(
        template="plotly_white",
        width=450,
        height=400,
        margin=dict(l=40, r=40, b=40, t=60),
        showlegend=False,
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f9f9f9",
    )
    fig_raw_cat.update_xaxes(categoryorder="total descending")  # or alphabetical

    fig_raw_cat_html = fig_raw_cat.to_html(full_html=False)

    # Another chart: distribution of ages in RAW
    raw_age_query = text("""
        SELECT age
        FROM bridgecart_customer_data.raw_customers
        WHERE age IS NOT NULL
    """)
    df_raw_age = pd.read_sql(raw_age_query, engine)

    fig_raw_age = px.histogram(
        df_raw_age,
        x="age",
        nbins=15,
        title="Age Distribution (Raw Layer)",
        labels={"age": "Age"},
    )
    fig_raw_age.update_layout(
        template="plotly_white",
        width=450,
        height=400,
        margin=dict(l=40, r=40, b=40, t=60),
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f9f9f9",
    )
    fig_raw_age_html = fig_raw_age.to_html(full_html=False)

    # ----------------------------------------------------
    # 3) EDA: SILVER LAYER
    # ----------------------------------------------------
    silver_pay_query = text("""
        SELECT payment_method, COUNT(*) as count_pay
        FROM bridgecart_customer_data.silver_customers
        GROUP BY payment_method
        ORDER BY count_pay DESC
        LIMIT 8
    """)
    df_silver_pay = pd.read_sql(silver_pay_query, engine)

    fig_silver_pay = px.pie(
        df_silver_pay,
        names="payment_method",
        values="count_pay",
        title="Payment Method Distribution (Silver)",
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig_silver_pay.update_layout(
        template="plotly_white",
        width=400,
        height=400,
        margin=dict(l=40, r=40, b=40, t=60),
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f9f9f9",
    )
    fig_silver_pay_html = fig_silver_pay.to_html(full_html=False)

    # Another EDA for silver: average discount by product category
    silver_disc_query = text("""
        SELECT product_category, AVG(discount_amount) AS avg_disc
        FROM bridgecart_customer_data.silver_customers
        WHERE discount_amount IS NOT NULL
        GROUP BY product_category
        ORDER BY avg_disc DESC
        LIMIT 8
    """)
    df_silver_disc = pd.read_sql(silver_disc_query, engine)

    fig_silver_disc = px.bar(
        df_silver_disc,
        x="product_category",
        y="avg_disc",
        title="Avg Discount by Product Category (Silver)",
        labels={"product_category": "Category", "avg_disc": "Avg Discount"},
        color="product_category",
    )
    fig_silver_disc.update_layout(
        template="plotly_white",
        width=400,
        height=400,
        margin=dict(l=40, r=40, b=40, t=60),
        showlegend=False,
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f9f9f9",
    )
    fig_silver_disc.update_traces(marker_line_color="black", marker_line_width=1)
    fig_silver_disc.update_xaxes(categoryorder="total descending")

    fig_silver_disc_html = fig_silver_disc.to_html(full_html=False)

    # ----------------------------------------------------
    # 4) EDA: GOLD LAYER
    # ----------------------------------------------------
    gold_disc_query = text("""
        SELECT 
          AVG(discount_amount) as avg_discount, 
          SUM(coupon_used::int) as total_coupons_used,
          COUNT(*) as total_rows
        FROM bridgecart_customer_data.gold_customers_segments
    """)
    df_gold_disc = pd.read_sql(gold_disc_query, engine)
    avg_discount = df_gold_disc["avg_discount"].iloc[0] or 0
    total_coupons_used = df_gold_disc["total_coupons_used"].iloc[0] or 0
    total_gold_rows = df_gold_disc["total_rows"].iloc[0]

    # Gauge chart for average discount in gold
    fig_disc = go.Figure(go.Indicator(
        mode="gauge+number",
        value=avg_discount,
        title={"text": "Avg Discount (Gold)"},
        gauge={"axis": {"range": [None, max(50, avg_discount*2)]}},
        domain={'x': [0, 1], 'y': [0, 1]}
    ))
    fig_disc.update_layout(
        template="plotly_dark",
        width=300,
        height=300,
        margin=dict(l=20, r=20, b=20, t=50),
    )
    fig_disc_html = fig_disc.to_html(full_html=False)

    # Another small indicator for total coupons used
    fig_coupons = go.Figure(go.Indicator(
        mode="number",
        value=total_coupons_used,
        title={"text": "Total Coupons Used (Gold)"},
    ))
    fig_coupons.update_layout(
        template="plotly_dark",
        width=300,
        height=300,
        margin=dict(l=20, r=20, b=20, t=50),
    )
    fig_coupons_html = fig_coupons.to_html(full_html=False)

    # ----------------------------------------------------
    # 5) SCD2 SEGMENT METRICS
    # ----------------------------------------------------
    scd2_query = text("""
        SELECT 
          id,
          segment,
          avg_clv,
          churn_rate,
          aov,
          segment_contribution,
          scd_start_date,
          scd_end_date,
          scd_is_current
        FROM bridgecart_customer_data.gold_segment_metrics_scd2
        ORDER BY segment, id
    """)
    df_scd2 = pd.read_sql(scd2_query, engine)

    # Filter only current records for bar/pie
    df_current = df_scd2[df_scd2["scd_is_current"] == True].copy()
    # Sort by segment ID
    df_current.sort_values("segment", inplace=True)

    # Bar chart: Average CLV by segment (current)
    fig_clv_bar = px.bar(
        df_current,
        x="segment",
        y="avg_clv",
        color="segment",
        title="Current Avg CLV by Segment",
        labels={"segment": "Segment ID", "avg_clv": "Average CLV"},
    )
    fig_clv_bar.update_traces(texttemplate="%{y:.2f}", textposition="outside")
    fig_clv_bar.update_layout(
        template="plotly_white",
        width=400,
        height=400,
        margin=dict(l=40, r=40, b=40, t=60),
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f9f9f9",
        xaxis=dict(type="category"),
    )
    fig_clv_bar_html = fig_clv_bar.to_html(full_html=False)

    # Donut chart: Segment contribution
    fig_contrib = px.pie(
        df_current,
        names="segment",
        values="segment_contribution",
        title="Segment Contribution (Current)",
        color_discrete_sequence=px.colors.qualitative.Set2,
        hole=0.4
    )
    fig_contrib.update_layout(
        width=400,
        height=400,
        margin=dict(l=40, r=40, b=40, t=60),
        showlegend=True,
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f9f9f9",
    )
    fig_contrib_html = fig_contrib.to_html(full_html=False)

    # Line chart: churn rate over time
    fig_churn = go.Figure()
    seg_list_sorted = sorted(df_scd2["segment"].unique())
    for seg_id in seg_list_sorted:
        seg_data = df_scd2[df_scd2["segment"] == seg_id].sort_values("scd_start_date")
        fig_churn.add_trace(go.Scatter(
            x=seg_data["scd_start_date"],
            y=seg_data["churn_rate"],
            mode="lines+markers",
            name=f"Segment {seg_id}"
        ))
    fig_churn.update_layout(
        title="Churn Rate Over Time (SCD2 History)",
        template="plotly_white",
        width=800,
        height=400,
        margin=dict(l=40, r=40, b=40, t=60),
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f9f9f9",
        xaxis_title="SCD Start Date",
        yaxis_title="Churn Rate (Proportion)",
    )
    fig_churn_html = fig_churn.to_html(full_html=False)

    # ----------------------------------------------------
    # HTML Template
    # ----------------------------------------------------
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>BridgeCart Analytics Dashboard</title>
        <!-- Google Fonts -->
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500&display=swap">
        <style>
            body {
              font-family: 'Roboto', sans-serif;
              font-size: 16px;
              background: #f0f0f0;
              margin: 20px;
              color: #333;
            }
            h1, h2, h3 {
              margin-top: 1rem;
              margin-bottom: 0.5rem;
            }
            .chart-container, .section-container {
              background: #fff;
              border: 1px solid #ddd;
              border-radius: 8px;
              padding: 20px;
              margin: 20px auto;
              max-width: 1100px;
            }
            .section-title {
              text-align: center;
              margin-bottom: 1rem;
            }
            .table {
              margin: 0 auto;
              border-collapse: collapse;
              width: 90%;
            }
            .table th, .table td {
              border: 1px solid #ccc;
              padding: 8px;
              text-align: left;
            }
            .table thead {
              background: #eee;
            }
            .drift-wrapper {
              display: flex;
              justify-content: center;
              align-items: center;
              flex-wrap: wrap;
            }
            .drift-card {
              background: #fafafa;
              border: 1px solid #ccc;
              border-radius: 8px;
              padding: 10px 20px;
              margin: 10px;
              width: 300px;
              text-align: center;
            }
            .flex-row {
              display: flex;
              flex-wrap: wrap;
              justify-content: center;
              align-items: flex-start;
              gap: 20px;
            }
            .chart-box {
              background: #fefefe;
              border: 1px solid #ddd;
              border-radius: 8px;
              padding: 10px;
              margin: 10px;
              box-shadow: 0 2px 3px rgba(0,0,0,0.1);
            }
            .chart-description {
              font-size: 0.9rem;
              color: #555;
              margin-top: 0.5rem;
            }
        </style>
    </head>
    <body>
      <h1>BridgeCart Analytics Dashboard</h1>

      <!-- Pipeline Drift & Execution Metrics -->
      <div class="section-container">
        <h2 class="section-title">Pipeline Drift & Execution Metrics</h2>
        <p style="text-align:center;">
          Below table shows the latest ingestion run for each layer, how many records existed before 
          the latest run, how many were newly added, and an approximate <b>CLV Drift</b> in the Gold 
          layer compared to the previous record. 
        </p>
        {{ drift_table_html|safe }}

        <div class="drift-wrapper">
          <div class="drift-card">
            <h3>CLV Drift (Latest)</h3>
            <p style="font-size:1.5rem;"><b>{{ clv_drift_value }}</b></p>
          </div>
        </div>
      </div>

      <!-- EDA: RAW LAYER -->
      <div class="section-container">
        <h2 class="section-title">Raw Layer EDA</h2>
        <div class="flex-row">
          <div class="chart-box">
            {{ fig_raw_cat_html|safe }}
            <div class="chart-description">
              This bar chart shows the top product categories in the raw layer. Higher bars indicate categories 
              with more frequent purchases or logs.
            </div>
          </div>
          <div class="chart-box">
            {{ fig_raw_age_html|safe }}
            <div class="chart-description">
              This histogram shows the distribution of customer ages in the raw data. 
              It helps identify the most common age ranges of our customers.
            </div>
          </div>
        </div>
      </div>

      <!-- EDA: SILVER LAYER -->
      <div class="section-container">
        <h2 class="section-title">Silver Layer EDA</h2>
        <div class="flex-row">
          <div class="chart-box">
            {{ fig_silver_pay_html|safe }}
            <div class="chart-description">
              A pie chart of payment methods in the silver layer. This helps see which methods 
              are most common after cleaning data (e.g., card, PayPal, COD, etc.).
            </div>
          </div>
          <div class="chart-box">
            {{ fig_silver_disc_html|safe }}
            <div class="chart-description">
              A bar chart showing the average discount amounts across different product categories 
              (in the silver dataset).
            </div>
          </div>
        </div>
      </div>

      <!-- EDA: GOLD LAYER -->
      <div class="section-container">
        <h2 class="section-title">Gold Layer EDA</h2>
        <p style="text-align:center;">
          We track key discount usage and total records in the final segmented data. 
        </p>
        <p style="text-align:center;">
          Rows in <b>gold_customers_segments</b>: <strong>{{ total_gold_rows }}</strong><br>
          Coupons Used (Total): <strong>{{ total_coupons_used }}</strong>
        </p>
        <div class="flex-row" style="justify-content:center;">
          <div class="chart-box">
            {{ fig_disc_html|safe }}
            <div class="chart-description">
              Gauge showing the average discount among all records in gold. 
              This indicates the typical discount customers receive.
            </div>
          </div>
          <div class="chart-box">
            {{ fig_coupons_html|safe }}
            <div class="chart-description">
              Total coupons used. This numeric indicator helps see how many 
              purchases utilized a coupon.
            </div>
          </div>
        </div>
      </div>

      <!-- SCD2 SEGMENT METRICS -->
      <div class="section-container">
        <h2 class="section-title">Segment Analysis (SCD2)</h2>
        <p style="text-align:center;">
          Below we display segment metrics tracked historically using a Type 2 SCD approach. 
          Current records (i.e., <b>scd_is_current=TRUE</b>) show the latest snapshot of each segment's performance. 
        </p>
        <div class="flex-row">
          <div class="chart-box">
            {{ fig_clv_bar_html|safe }}
            <div class="chart-description">
              Average CLV by segment for current data. This compares each segment's typical 
              spending behavior over time.
            </div>
          </div>
          <div class="chart-box">
            {{ fig_contrib_html|safe }}
            <div class="chart-description">
              Each segment's contribution to total revenue. Helps identify the segments that drive 
              most of the sales.
            </div>
          </div>
        </div>
        <div class="chart-box" style="margin:auto; margin-top:20px; width:85%;">
            {{ fig_churn_html|safe }}
            <div class="chart-description" style="text-align:center;">
              Churn Rate Over Time (Line Chart). Each line represents a segment's churn trend (SCD2 history).
            </div>
        </div>
      </div>

      <hr style="margin-top:40px;">
      <p style="text-align:center; font-size:0.9rem; color:#666;">
        &copy; 2023 BridgeCart Analytics. All rights reserved.
      </p>
    </body>
    </html>
    """

    # Render
    return render_template_string(
        html_template,
        drift_table_html=drift_table_html,
        clv_drift_value=round(clv_drift_value, 2),
        fig_raw_cat_html=fig_raw_cat_html,
        fig_raw_age_html=fig_raw_age_html,
        fig_silver_pay_html=fig_silver_pay_html,
        fig_silver_disc_html=fig_silver_disc_html,
        fig_disc_html=fig_disc_html,
        fig_coupons_html=fig_coupons_html,
        total_gold_rows=total_gold_rows,
        total_coupons_used=total_coupons_used,
        fig_clv_bar_html=fig_clv_bar_html,
        fig_contrib_html=fig_contrib_html,
        fig_churn_html=fig_churn_html
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5010, debug=True)
