# reporting/app.py

import os
import pandas as pd
import plotly.graph_objects as go
from flask import Flask, render_template_string
from sqlalchemy import create_engine

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

    # EXAMPLE: Query SCD2 table
    query = """
        SELECT 
          segment,
          avg_clv,
          churn_rate,
          aov,
          segment_contribution,
          scd_start_date,
          scd_end_date,
          scd_is_current
        FROM bridgecart_customer_data.gold_segment_metrics_scd2
        ORDER BY id
    """
    df = pd.read_sql(query, engine)

    # Basic chart: show the average CLV by segment
    fig = go.Figure(data=[
        go.Bar(
            x=df["segment"].unique(),
            y=df.groupby("segment")["avg_clv"].mean(),
            name="Average CLV"
        )
    ])
    fig.update_layout(title="Average CLV by Segment")

    # Build HTML
    chart_html = fig.to_html(full_html=False)

    # Another example: churn_rate line chart
    churn_fig = go.Figure()
    for seg_id in df["segment"].unique():
        seg_data = df[df["segment"] == seg_id].sort_values("scd_start_date")
        churn_fig.add_trace(go.Scatter(
            x=seg_data["scd_start_date"],
            y=seg_data["churn_rate"],
            mode="lines+markers",
            name=f"Segment {seg_id} Churn"
        ))
    churn_fig.update_layout(title="Churn Rate Over Time")
    churn_html = churn_fig.to_html(full_html=False)

    # Simple HTML template
    html_template = """
    <html>
      <head>
        <title>BridgeCart Metrics Dashboard</title>
      </head>
      <body>
        <h1>BridgeCart SCD2 Segment Metrics</h1>
        <h2>Bar Chart: Average CLV by Segment</h2>
        {{ chart_html|safe }}

        <h2>Line Chart: Churn Rate Over Time (by segment)</h2>
        {{ churn_html|safe }}

        <hr>
        <p>Data row count: {{ row_count }}</p>
        <p>Example from gold_segment_metrics_scd2 (SCD2) table.</p>
      </body>
    </html>
    """
    row_count = len(df)
    return render_template_string(html_template,
                                  chart_html=chart_html,
                                  churn_html=churn_html,
                                  row_count=row_count)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5010, debug=True)
