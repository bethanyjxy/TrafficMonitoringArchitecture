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
    description='Run a Spark job to process historical data',
    schedule_interval=None,  # Can be '0 12 * * *' to run every day at noon
)

# Task to run Spark job from the command line
run_spark_job = BashOperator(
    task_id='historical_dag',
    bash_command='spark-submit --master local /opt/airflow/app/spark/batch/hist_process.py',
    dag=dag,
)
