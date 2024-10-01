from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
# Import your script functions
from kafka.producer.producer import fetch_and_produce_data  # Assuming this is the main function in your producer script
from kafka.consumer.kafka_erp_consumer import consume_erp_messages
from kafka.consumer.kafka_incidents_consumer import consume_incident_messages
from kafka.consumer.kafka_to_hdfs import create_directory

# Import Kafka consumer initialization
from kafka.consumer.consumer_config import initialize_consumer, close_consumer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

dag = DAG(
    'traffic_data_pipeline',
    default_args=default_args,
    description='A DAG for the traffic data pipeline',
    schedule_interval='@daily',  # Adjust as needed
)

# Task 1: Fetch data and produce to Kafka
producer_task = PythonOperator(
    task_id='produce_to_kafka',
    python_callable=fetch_and_produce_data,
    op_kwargs={'broker': os.environ.get('KAFKA_BOOTSTRAP_SERVERS')},
    dag=dag,
)


# Task to ensure HDFS directory exists
def create_hdfs_dir():
    directory_path = "/user/data/traffic_data/{}/".format(datetime.now().strftime("%Y-%m-%d"))
    create_directory(directory_path)

create_hdfs_dir_task = PythonOperator(
    task_id='create_hdfs_directory',
    python_callable=create_hdfs_dir,
    dag=dag,
)

# Task 2: Consume data from Kafka
def run_consumer(consume_function, topic):
    consumer = initialize_consumer(topic)
    try:
        consume_function(consumer)
    finally:
        close_consumer(consumer)

# Task for ERP consumer
erp_consumer_task = PythonOperator(
    task_id='consume_erp_data',
    python_callable=run_consumer,
    op_kwargs={'consume_function': consume_erp_messages, 'topic': 'traffic_erp'},
    dag=dag,
)

# Task for Incident consumer
incident_consumer_task = PythonOperator(
    task_id='consume_incident_data',
    python_callable=run_consumer,
    op_kwargs={'consume_function': consume_incident_messages, 'topic': 'traffic_incidents'},
    dag=dag,
)

# Task 3: Process data with Spark and store in PostgreSQL
# spark_task = PythonOperator(
#     task_id='process_with_spark',
#     python_callable=process_data,
#     op_kwargs={'postgres_conn': os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')},
#     dag=dag,
# )



# Set up task dependencies# Set up task dependencies
producer_task >> create_hdfs_dir_task
create_hdfs_dir_task >> [erp_consumer_task, incident_consumer_task]
