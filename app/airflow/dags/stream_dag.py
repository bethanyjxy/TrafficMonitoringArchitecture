from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 23),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'realtime_pipeline',
    default_args=default_args,
    description='Simple orchestration DAG for Kafka and Spark processing',
    schedule_interval=timedelta(hours=1), 
    catchup=False,
    max_active_runs=1,  # Ensures multiple DAG runs can run in parallel
)

# Task 1: Start Kafka Producer
def start_kafka_producer():
    """Starts the Kafka producer by calling the relevant script."""
    cmd = ["python3", "app/kafka/producer/producer.py"]
    subprocess.run(cmd, check=True)

start_producer_task = PythonOperator(
    task_id='start_kafka_producer',
    python_callable=start_kafka_producer,
    dag=dag,
)

# Task 2: Start multiple Kafka consumers
def start_kafka_consumer(consumer_script):
    """Starts the Kafka consumer by calling the relevant consumer script."""
    cmd = ["python3", f"app/kafka/consumer/{consumer_script}"]
    subprocess.run(cmd, check=True)

# Define tasks for each consumer
consumers = ['kafka_vms_consumer.py', 'kafka_images_consumer.py', 'kafka_speedbands_consumer.py', 'kafka_incidents_consumer.py', 'kafka_erp_consumer.py']

for consumer in consumers:
    task = PythonOperator(
        task_id=f'start_{consumer.split(".")[0]}',
        python_callable=start_kafka_consumer,
        op_kwargs={'consumer_script': consumer},
        dag=dag,
    )
    start_producer_task >> task  # Set task dependencies


# Task 3: Start Spark streaming
start_spark_streaming = BashOperator(
    task_id='start_spark_streaming',
    bash_command="""
        echo "Starting Kafka to Spark Streaming..."
        spark-submit \
          --conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
          --conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
          --jars /opt/spark/jars/postgresql-42.2.18.jar \
          app/spark/realtime/postgres_stream.py
    """,
    dag=dag,
)

# Health Check Tasks: Check if Kafka producers/consumers are running
def check_process_health(process_name):
    """Placeholder function to check if the given process is running."""
    try:
        result = subprocess.run(['pgrep', '-f', process_name], check=True, capture_output=True)
        if result.stdout:
            print(f"{process_name} is running.")
        else:
            raise Exception(f"{process_name} is not running.")
    except Exception as e:
        print(f"Health check failed for {process_name}: {e}")

check_producer_health = PythonOperator(
    task_id='check_producer_health',
    python_callable=check_process_health,
    op_kwargs={'process_name': 'kafka_producer'},
    dag=dag,
)

check_vms_consumer_health = PythonOperator(
    task_id='check_vms_consumer_health',
    python_callable=check_process_health,
    op_kwargs={'process_name': 'kafka_vms_consumer'},
    dag=dag,
)

check_images_consumer_health = PythonOperator(
    task_id='check_images_consumer_health',
    python_callable=check_process_health,
    op_kwargs={'process_name': 'kafka_images_consumer'},
    dag=dag,
)

check_speedbands_consumer_health = PythonOperator(
    task_id='check_speedbands_consumer_health',
    python_callable=check_process_health,
    op_kwargs={'process_name': 'kafka_speedbands_consumer'},
    dag=dag,
)

check_incidents_consumer_health = PythonOperator(
    task_id='check_incidents_consumer_health',
    python_callable=check_process_health,
    op_kwargs={'process_name': 'kafka_incidents_consumer'},
    dag=dag,
)
check_erp_consumer_health = PythonOperator(
    task_id='check_erp_consumer_health',
    python_callable=check_process_health,
    op_kwargs={'process_name': 'kafka_erp_consumer'},
    dag=dag,
)
# Set the task order: Producer starts first, then consumers, then Spark processing
start_producer_task >> [check_incidents_consumer_health, check_erp_consumer_health ,check_producer_health, check_vms_consumer_health, check_images_consumer_health, check_speedbands_consumer_health] >> start_spark_streaming
