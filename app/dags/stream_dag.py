from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 23),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'kafka_spark_dag',
    default_args=default_args,
    description='A DAG to start Kafka consumers, producer, and Spark streaming',
    schedule_interval='@once', 
    catchup=False)

# Task to start Kafka Producer
start_kafka_producer = BashOperator(
        task_id='start_kafka_producer',
        bash_command="""
        echo "Starting Kafka Producer..."
        python3 app/kafka/producer/producer.py &
        """,
    dag=dag,
)

# Task to start Kafka consumers
start_kafka_consumer = BashOperator(
    task_id='start_kafka_consumer',
    bash_command="""
        echo "Starting Kafka Consumer..."
        python3 app/kafka/consumer/kafka_incidents_consumer.py &
        python3 app/kafka/consumer/kafka_images_consumer.py &
        python3 app/kafka/consumer/kafka_speedbands_consumer.py &
        python3 app/kafka/consumer/kafka_erp_consumer.py &
        python3 app/kafka/consumer/kafka_vms_consumer.py &
        
    """,
    dag=dag,
)

# Task to start Spark streaming
start_spark_streaming = BashOperator(
    task_id='start_spark_streaming',
    bash_command="""
        echo "Starting Kafka to Spark Streaming..."
        spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
          --jars /opt/spark/jars/postgresql-42.2.18.jar \
          app/spark/realtime/postgres_stream.py
    """,
    dag=dag,
)

# Set the task order
start_kafka_producer >> start_kafka_consumer >> start_spark_streaming

