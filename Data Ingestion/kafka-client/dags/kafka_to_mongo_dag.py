from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'spark_kafka_to_mongo',
    default_args=default_args,
    schedule_interval='@hourly',
)

spark_task = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit /path/to/kafka_to_mongo.py',
    dag=dag,
)
