from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'clear_airflow_logs',
    default_args=default_args,
    description='DAG to clear Airflow logs every day',
    schedule_interval=timedelta(days=1),  # Runs once every day
    start_date=datetime(2024, 12, 7),  # Adjust as needed
    catchup=False,  # Don't run for past dates
) as dag:

    # Define the task to clear the logs
    clear_logs = BashOperator(
        task_id='clear_logs_task',
        bash_command='rm -rf /opt/airflow/logs/*',  # Command to remove logs
        dag=dag,
    )