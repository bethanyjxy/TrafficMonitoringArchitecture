from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import subprocess

def run_spark_streaming():
    """Run the Spark streaming job using subprocess."""
    try:
        result = subprocess.run(
            [
                "spark-submit",
                "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1",
                "--jars", "/opt/spark/jars/postgresql-42.2.18.jar",
                "/opt/airflow/app/spark/realtime/postgres_stream.py"
            ],
            stdout=subprocess.PIPE,  # Capture stdout
            stderr=subprocess.PIPE,  # Capture stderr
            text=True,  # Ensure output is in string format
            check=True
        )
        print(result.stdout)  # Log stdout
    except subprocess.CalledProcessError as e:
        print(f"Spark job failed with exit code {e.returncode}: {e.stderr}")
        raise

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(seconds=4),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'spark_postgres_stream_dag',
    default_args=default_args,
    max_active_runs=1,  # Only one active run at a time
    concurrency=1,  # Ensure one task runs at a time
    description='Run Spark streaming job to process Kafka data and store in PostgreSQL',
    schedule_interval='@once',  # Run once and keep streaming
    catchup=False,
)

# Use PythonOperator to run the Spark streaming job
run_spark_postgres_stream = PythonOperator(
    task_id='run_spark_postgres_stream',
    python_callable=run_spark_streaming,
    execution_timeout=timedelta(hours=2),
    dag=dag,
)
