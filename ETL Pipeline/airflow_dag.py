
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
}

with DAG('data_ingestion_pipeline', default_args=default_args, schedule_interval=None) as dag:

    start_kafka_customers = BashOperator(
        task_id='send_customers_to_kafka',
        bash_command='python kafka_producers/kafka_producer_customers.py'
    )

    start_kafka_orders = BashOperator(
        task_id='send_orders_to_kafka',
        bash_command='python kafka_producers/kafka_producer_orders.py'
    )

    start_kafka_products = BashOperator(
        task_id='send_products_to_kafka',
        bash_command='python kafka_producers/kafka_producer_products.py'
    )

    start_spark_job = BashOperator(
        task_id='start_spark_ingestion',
        bash_command='spark-submit spark_ingest_script.py'
    )

    [start_kafka_customers, start_kafka_orders, start_kafka_products] >> start_spark_job
