"""
Airflow DAG to:
1. Produce synthetic data with Faker into Kafka.
2. Consume from Kafka => land incrementally into raw table in DB (with IST timestamps).
3. Transform/clean => incrementally load to silver table in DB.
4. Segment => incrementally load gold segments, and manage SCD2 in segment metrics.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from customer_segmentation.data_generation import generate_kafka_data
from customer_segmentation.ingestion import consume_and_store
from customer_segmentation.transformation import transform_and_clean
from customer_segmentation.segmentation import segment_and_load

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
# 2. Consume Data from Kafka => RAW
##############################
def consume_raw_data(**context):
    consume_and_store(
        topic=KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="bridgecart_consumer_group",
        timeout_ms=5000
    )

##############################
# Build the DAG
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
        python_callable=consume_raw_data,
    )

    # For transform, we call the function in transformation.py
    transform_task = PythonOperator(
        task_id="transform_and_clean",
        python_callable=transform_and_clean,
    )

    # For segment, we call the function in segmentation.py
    segment_task = PythonOperator(
        task_id="segment_and_load",
        python_callable=segment_and_load,
    )

    # Link tasks
    produce_task >> consume_task >> transform_task >> segment_task
