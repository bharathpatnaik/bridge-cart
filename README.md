# bridge-cart

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

# Bridge-Cart Customer Segmentation

## Overview

This project aims to build a configurable ETL (or ELT) pipeline to process and segment customer data for an e-commerce platform (BridgeCart). This pipeline covers:

1. **Data Generation** (using Faker + Kafka).
2. **Data Ingestion** (Kafka => landing in Raw Layer).
3. **Data Cleaning/Transformation** (Silver Layer).
4. **Feature Engineering** (CLV, standardized features).
5. **Segmentation** (using K-Means).
6. **Loading** results into a reporting table (Gold Layer).
7. **KPIs** (4 selected):
   - **Customer Lifetime Value Growth by Segment**
   - **Customer Churn Rate by Segment**
   - **Average Order Value (AOV) by Segment**
   - **Segment Contribution to Total Sales**

We demonstrate an example medallion architecture (Raw, Silver, Gold) plus an analytical step with scikit-learn. We also show how to incrementally ingest data from Kafka, so you can run the pipeline multiple times and only process newly arrived records.

## Architecture Diagram

```txt
               +----------+
               |  Faker   |
               |  (Data)  |
               +----+-----+
                    | (gen 'n' records)
                    v
+-------------------+-----------------+
|       Kafka Topic (customers)       |  <-- Task 1: Produce data
+-------------------+-----------------+
                    |
                    v
       +------------------------+
       |  Airflow DAG          |
       |  Task 2: Consume      |
       |  => Raw Layer         |
       +------------------------+
                    |
                    v
       +------------------------+
       |  Task 3: Transform    |
       |  => Silver Layer      |
       +------------------------+
                    |
                    v
       +------------------------+
       |  Task 4: Segmentation |
       |  => Gold Layer        |
       +------------------------+
                    |
                    v
       +------------------------+
       | Reporting/Analysis    |
       +------------------------+
```

## Project Organization

```
bridge-cart/
├── LICENSE
├── Makefile
├── README.md
├── docker-compose.yml     <- to use Docker for Kafka, Airflow, Postgres, etc.
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
│   └── Dockerfile          <- for containerizing Airflow
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

--------

