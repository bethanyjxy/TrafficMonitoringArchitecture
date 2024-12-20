from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(seconds=4),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Retry once if the task fails
}

# Initialize the DAG
dag = DAG(
    'spark_postgres_stream_dag2',
    default_args=default_args,
    max_active_runs=1,  # Only one active run at a time
    concurrency=1,  # Ensure one task runs at a time
    description='Run Spark streaming job to process Kafka data and store in PostgreSQL',
    schedule_interval='@once',  # Run once and keep streaming
    catchup=False,
)

# Define the SparkSubmitOperator to run the Spark streaming job
run_spark_postgres_stream = SparkSubmitOperator(
    task_id='run_spark_postgres_stream',
    application='/opt/airflow/app/spark/realtime/postgres_stream.py',
    name='spark_postgres_stream_job',
    conn_id='spark_default',
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1',  # Include Kafka integration package
    jars='/opt/spark/jars/postgresql-42.2.18.jar',  # Path to PostgreSQL JDBC jar
    execution_timeout=timedelta(days=30),
    dag=dag,
)
