"""
GoalFlow Daily Pipeline DAG
Author: Mohamed Chaari

Orchestrates the ingestion, processing, and reporting tasks for GoalFlow.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from loguru import logger

def task_failure_callback(context):
    """Callback function executed on task failure."""
    task_id = context['task'].task_id
    # We can use Airflow's built-in logging or loguru
    # Here we'll just print so Airflow captures it in logs
    print(f"Task {task_id} failed")

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'on_failure_callback': task_failure_callback
}

# Add doc_md describing the pipeline
dag_docs = """
# GoalFlow Daily Pipeline
This DAG orchestrates the football analytics data pipeline:
1. **Ingestion**: Fetches data from api-sports.io and produces to Kafka.
2. **Processing**: Consumes data from Kafka using PySpark, computes metrics, and writes to PostgreSQL.
3. **Analytics**: Generates a Plotly HTML report from the processed data.
"""

with DAG(
    dag_id='goalflow_daily_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    doc_md=dag_docs,
    tags=['goalflow', 'football']
) as dag:

    ingest_task = BashOperator(
        task_id='ingest_task',
        bash_command='docker exec ingestion python /app/producer.py'
    )

    process_task = BashOperator(
        task_id='process_task',
        bash_command='docker exec spark spark-submit /app/processor.py'
    )

    report_task = BashOperator(
        task_id='report_task',
        bash_command='docker exec analytics python /app/report.py'
    )

    # Set dependencies
    ingest_task >> process_task >> report_task
