# Bridge-Cart Customer Segmentation

[![CCDS Project template](https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter)](https://cookiecutter-data-science.drivendata.org/)

Welcome to **Bridge-Cart**, an end-to-end customer segmentation pipeline. This project shows how to generate synthetic data, ingest it into a **Medallion Architecture** (Raw → Silver → Gold), and perform analytics—ultimately segmenting e-commerce customers.

## What Does It Do?

1. **Data Generation**: We use [Faker](https://faker.readthedocs.io/) to create realistic customer and purchase data.
2. **Ingestion**: Data lands in a **Raw** layer (optionally via Kafka).
3. **Cleaning & Transformation**: In the **Silver** layer, we remove duplicates, fill missing values, and unify data formats.
4. **Feature Engineering**: Compute features like **CLV** (Customer Lifetime Value).
5. **Segmentation**: Apply K-Means clustering to group customers by behavior.
6. **Gold Layer**: Store the final, enriched data for reporting or additional analysis.
7. **KPIs**: Track churn rate, average order value, segment contribution, and more.

## How to Explore

- **[docs/](./docs)**: Contains several short guides explaining each pipeline stage—from Raw ingestion to reporting.
- **[airflow/dags/](./airflow/dags)**: The Airflow DAG that orchestrates ingestion, transformation, and load.
- **[notebooks/](./notebooks)**: Example Jupyter notebook(s) for data exploration.
- **[customer_segmentation/](./customer_segmentation)**: Core source code for dataset generation, feature engineering, and modeling.

## Getting Started

1. **Install Dependencies**: Use `pip install -r requirements.txt`.
2. **Start Docker Services**: `docker-compose up -d` (brings up Kafka, Airflow, Postgres).
3. **Run the Pipeline**:
   - Access Airflow at `http://localhost:8080`
   - Access reports at `http://localhost:5010`
   - Access Postgres at `http://localhost:5432`

## Highlights

- **Incremental Ingestion**: The pipeline only processes new data each run, using offsets or timestamps.
- **SCD2 (Slowly Changing Dimensions)**: We keep historical versions of key metrics in the Gold layer for tracking changes over time.
- **Flexible**: we can swap out Kafka or Airflow as desired, or deploy to other cloud/data platforms.

## Directory Structure


```
bridge-cart/
├── LICENSE
├── Makefile
├── README.md
├── docker-compose.yml
├── data
│   ├── external
│   ├── interim
│   ├── processed
│   └── raw
├── docs
├── models
├── notebooks
│   └── 1.0-jqp-initial-data-exploration.ipynb
├── pyproject.toml
├── references
├── reports
│   └── figures
├── requirements.txt
├── setup.cfg
├── airflow
│   ├── dags
│   │   └── customer_segmentation_dag.py
│   └── Dockerfile
└── customer_segmentation
    ├── __init__.py
    ├── config.py
    ├── dataset.py
    ├── features.py
    ├── modeling
    │   ├── __init__.py
    │   ├── predict.py
    │   └── train.py
    └── plots.py

```