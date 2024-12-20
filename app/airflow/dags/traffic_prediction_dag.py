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
    'traffic_prediction',
    default_args=default_args,
    description='DAG for traffic Prediction',
    schedule='0 23 * * *', 
)

# Task to run the Python script
run_daily_incident_report = BashOperator(
    task_id='run_daily_incident_report',
    bash_command='python3 /opt/airflow/app/spark/batch/traffic_prediction.py',  # Adjust the path to your script
    dag=dag,
)