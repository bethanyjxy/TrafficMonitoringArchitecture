from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(seconds=0),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

# Initialize the DAG
dag = DAG(
    'kafka_consumer_dag',
    default_args=default_args,
    description='Run Kafka consumer scripts to consume traffic data',
    schedule='@daily',  # Set to None to trigger manually
)

# Task to run Kafka consumer for traffic incidents
run_incidents_consumer = BashOperator(
    task_id='run_kafka_incidents_consumer',
    bash_command='python3 /opt/airflow/app/kafka/consumer/kafka_incidents_consumer.py',
    do_xcom_push=True,  
    dag=dag,
)

# Task to run Kafka consumer for traffic images
run_images_consumer = BashOperator(
    task_id='run_kafka_images_consumer',
    bash_command='python3 /opt/airflow/app/kafka/consumer/kafka_images_consumer.py',
    do_xcom_push=True,  
    dag=dag,
)

# Task to run Kafka consumer for traffic speedbands
run_speedbands_consumer = BashOperator(
    task_id='run_kafka_speedbands_consumer',
    bash_command='python3 /opt/airflow/app/kafka/consumer/kafka_speedbands_consumer.py',
    do_xcom_push=True,  
    dag=dag,
)

# Task to run Kafka consumer for traffic vms
run_vms_consumer = BashOperator(
    task_id='run_kafka_vms_consumer',
    bash_command='python3 /opt/airflow/app/kafka/consumer/kafka_vms_consumer.py',
    do_xcom_push=True,  
    dag=dag,
)



# Set dependencies, consumers run in parallel
[run_incidents_consumer, run_images_consumer, run_speedbands_consumer, run_vms_consumer] 
