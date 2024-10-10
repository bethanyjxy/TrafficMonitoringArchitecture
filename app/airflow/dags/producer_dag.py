from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

# Initialize the DAG
dag = DAG(
    'kafka_producer_dag',
    default_args=default_args,
    description='Run Kafka producer scripts to fetch and produce traffic data',
    schedule_interval=None,  # Set a cron schedule or None to trigger manually
)

# Task to run Kafka producer script for traffic incidents
run_incidents_producer = BashOperator(
    task_id='run_kafka_incidents_producer',
    bash_command='python3 /opt/airflow/app/kafka/producer/fetch_and_produce_data.py',
    dag=dag,
)
