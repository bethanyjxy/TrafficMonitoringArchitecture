from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Initialize DAG
dag = DAG(
    'historical_dag',
    default_args=default_args,
    description='Run a Spark job to process daily incident data',
    schedule_interval=None,  # Can be '0 12 * * *' to run every day at noon
)

# Initialize the DAG
dag = DAG(
    'daily_incident_batch',
    default_args=default_args,
    description='Daily batch processing for traffic incident reports',
    schedule_interval='@daily',  # Run once a day at midnight
)

# Task to run the Python script
run_daily_incident_report = BashOperator(
    task_id='run_daily_incident_report',
    bash_command='python3 /opt/airflow/app/spark/batch/daily_incident.py',  # Adjust the path to your script
    dag=dag,
)