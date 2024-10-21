from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date':  pendulum.today('UTC').add(seconds=4),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Retry once if the task fails
}

# Initialize the DAG
dag = DAG(
    'spark_postgres_stream_dag',
    default_args=default_args,
    description='Run Spark streaming job to process Kafka data and store in PostgreSQL',
    schedule='@daily',  # Set this to your desired schedule, e.g., every 10 minutes
)

# Task to run the Spark streaming job
run_spark_postgres_stream = BashOperator(
    task_id='run_spark_postgres_stream',
    bash_command=''' 
        spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
        --jars /opt/spark/jars/postgresql-42.2.18.jar \
        /opt/airflow/app/spark/realtime/postgres_stream.py \
        2>&1
    ''',
    execution_timeout=timedelta(hours=2),  # Adjust based on expected runtime of the Spark job
    dag=dag,
)
