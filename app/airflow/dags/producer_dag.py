from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date':  pendulum.today('UTC').add(seconds=2),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

# Initialize the DAG
dag = DAG(
    'kafka_producer_dag',
    default_args=default_args,
    description='Run Kafka producer scripts to fetch and produce traffic data',
    schedule='@daily',  # Set a cron schedule or None to trigger manually
)

# Task to run Kafka producer script for traffic incidents
run_incidents_producer = BashOperator(
    task_id='run_kafka_incidents_producer',
    bash_command='python3 /opt/airflow/app/kafka/producer/producer.py',
    dag=dag,
)
