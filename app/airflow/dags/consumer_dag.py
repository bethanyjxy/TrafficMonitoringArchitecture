from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import subprocess

# Define a function to run the external Kafka consumer scripts
def run_consumer_script(script_path):
    """Run the external Kafka consumer script using subprocess."""
    try:
        subprocess.run(
            ["python3", script_path],
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Script {script_path} failed with exit code {e.returncode}: {e.output}")

# Define default arguments
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
    'kafka_consumer_dag',
    default_args=default_args,
    description='Run Kafka consumer scripts to consume traffic data',
    schedule_interval='@once',  # Run only once, let the scripts loop
    catchup=False,
)

# Task to run Kafka consumer for traffic incidents
run_incidents_consumer = PythonOperator(
    task_id='run_kafka_incidents_consumer',
    python_callable=run_consumer_script,
    op_args=['/opt/airflow/app/kafka/consumer/kafka_incidents_consumer.py'],
    dag=dag,
)

# Task to run Kafka consumer for traffic images
run_images_consumer = PythonOperator(
    task_id='run_kafka_images_consumer',
    python_callable=run_consumer_script,
    op_args=['/opt/airflow/app/kafka/consumer/kafka_images_consumer.py'],
    dag=dag,
)

# Task to run Kafka consumer for traffic speedbands
run_speedbands_consumer = PythonOperator(
    task_id='run_kafka_speedbands_consumer',
    python_callable=run_consumer_script,
    op_args=['/opt/airflow/app/kafka/consumer/kafka_speedbands_consumer.py'],
    dag=dag,
)

# Task to run Kafka consumer for traffic vms
run_vms_consumer = PythonOperator(
    task_id='run_kafka_vms_consumer',
    python_callable=run_consumer_script,
    op_args=['/opt/airflow/app/kafka/consumer/kafka_vms_consumer.py'],
    dag=dag,
)
