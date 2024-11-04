from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    'delete_old_records',
    default_args=default_args,
    description='DAG to delete old records from Postgres tables',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task to delete records from incident_table
    delete_from_incident_table = PostgresOperator(
        task_id='delete_from_incident_table',
        postgres_conn_id='postgres_default',  # Use the connection ID you set up
        sql="DELETE FROM incident_table WHERE timestamp < NOW() - INTERVAL '3 days';"
    )

    # Task to delete records from speedbands_table
    delete_from_speedbands_table = PostgresOperator(
        task_id='delete_from_speedbands_table',
        postgres_conn_id='postgres_default',
        sql="DELETE FROM speedbands_table WHERE timestamp < NOW() - INTERVAL '3 days';"
    )

    # Task to delete records from vms_table
    delete_from_vms_table = PostgresOperator(
        task_id='delete_from_vms_table',
        postgres_conn_id='postgres_default',
        sql="DELETE FROM vms_table WHERE timestamp < NOW() - INTERVAL '3 days';"
    )

    # Task to delete records from image_table
    delete_from_image_table = PostgresOperator(
        task_id='delete_from_image_table',
        postgres_conn_id='postgres_default',
        sql="DELETE FROM image_table WHERE img_timestamp < NOW() - INTERVAL '3 days';"
    )

    # Define task dependencies (optional, depending on if they need to run sequentially)
    delete_from_incident_table >> delete_from_speedbands_table >> delete_from_vms_table >> delete_from_image_table