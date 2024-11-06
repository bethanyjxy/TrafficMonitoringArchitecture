from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('Asia/Singapore').add(days=-1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Initialize the DAG
dag = DAG(
    'daily_incident_batch',
    default_args=default_args,
    description='Daily batch processing for traffic incident reports',
<<<<<<< HEAD
    schedule='0 23 * * *',  # Run daily at 3 PM UTC (11 PM Singapore Time)
=======
    schedule='0 22 * * *',  # Run daily at 3 PM UTC (11 PM Singapore Time)
>>>>>>> 2323af9ab9e23e8ea1a2768e77ddb73f848ee25a
)

# Task to run the Python script
run_daily_incident_report = BashOperator(
    task_id='run_daily_incident_report',
    bash_command='python3 /opt/airflow/app/spark/batch/daily_incident.py',  # Adjust the path to your script
    dag=dag,
)
