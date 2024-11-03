from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import subprocess

# Define a function to run the external Python script
def run_producer_script():
    """Run the Kafka producer script using subprocess."""
    try:
        # Use subprocess to run the external Python script
        subprocess.run(
            ["python3", "/opt/airflow/app/kafka/producer/producer.py"],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Script failed with exit code {e.returncode}: {e.output}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC'),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# Initialize the DAG
dag = DAG(
    'kafka_producer_dag',
    default_args=default_args,
    description='Run Kafka producer scripts to fetch and produce traffic data',
    schedule_interval='@once',  # Run once and let the script handle looping
    catchup=False,
    max_active_runs=1,  # Only one active run at a time
    concurrency=1,  # Ensure one task runs at a time
)

# Use PythonOperator to run the producer script with subprocess
run_producer_task = PythonOperator(
    task_id='run_kafka_producer',
    python_callable=run_producer_script,
    dag=dag,
)
