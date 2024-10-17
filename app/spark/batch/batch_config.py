import os
import json
import time
import logging
import psycopg2
from datetime import datetime
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from postgresql.postgres_config import POSTGRES_DB

# Initialize logging
logging.basicConfig(level=logging.INFO)

# HDFS configuration
NAMENODE_HOST = os.environ.get('HDFS_NAMENODE_HOST', 'namenode')
NAMENODE_PORT = int(os.environ.get('HDFS_NAMENODE_PORT', 9870))
HDFS_USER = os.environ.get('HDFS_USER', 'hadoop')
HDFS_DIRECTORY = os.environ.get('HDFS_DIRECTORY', '/user/hadoop/traffic_data/')

# Construct HDFS URL
hdfs_url = f"http://{NAMENODE_HOST}:{NAMENODE_PORT}"

# Initialize HDFS client
hdfs_client = InsecureClient(hdfs_url, user=HDFS_USER)

###### SPARK ######
def create_spark_session(app_name):
    """Creates a Spark session."""
    logging.info(f"Creating Spark session for {app_name}")
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    
###### POSTGRESQL ######
def get_postgres_connection():
    """Returns a connection to PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB['dbname'],
            user=POSTGRES_DB['user'],
            password=POSTGRES_DB['password'],
            host=POSTGRES_DB['host'],
            port=POSTGRES_DB['port']
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        raise
    
def create_table(create_table_query, conn):
    """Creates table if it does not exist."""
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        logging.info("Table created or already exists.")
    except Exception as e:
        logging.error(f"Error creating table in PostgreSQL: {e}")
        if conn:
            conn.rollback()

def insert_table(data, insert_query, conn):
    """Inserts data into PostgreSQL."""
    try:
        cursor = conn.cursor()
        logging.info("Inserting data into PostgreSQL...")
        cursor.execute(insert_query, data)
        conn.commit()
        cursor.close()
        logging.info("Data inserted successfully.")
    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")
        if conn:
            conn.rollback()


###### HDFS ######            
def send_to_hdfs(topic, data):
    """Send data to HDFS."""
    file_path = os.path.join(HDFS_DIRECTORY, f"{topic}.json")
    try:
        # Check if HDFS is reachable
        hdfs_client.status('/')  # Check if HDFS is running
        logging.info("Connected to HDFS!")

        # Check if the file exists, if not, create it
        if not hdfs_client.status(file_path, strict=False):
            logging.info(f"File {file_path} not found, creating it...")
            # Create the file if it does not exist
            with hdfs_client.write(file_path, encoding='utf-8') as writer:
                writer.write('')  # Create an empty file
                
        # Prepare data with a timestamp
        timestamp = datetime.now().isoformat()  
        data_timestamp = {
            "timestamp": timestamp,
            **data  # Unpack the existing data into the new dictionary
        }

        # Append data to the file
        with hdfs_client.write(file_path, encoding='utf-8', append=True) as writer:
            writer.write(json.dumps(data_timestamp) + "\n")
        logging.info(f"Data sent to HDFS for topic '{topic}' with timestamp '{timestamp}'")
        
    except Exception as e:
        logging.error(f"Error sending data to HDFS: {e}")
        logging.error(f"Check if HDFS is running and accessible at {NAMENODE_HOST}:{NAMENODE_PORT}. Ensure the directory '{HDFS_DIRECTORY}' exists and is writable.")


###### Others ######
def generate_id():
    return str(int(time.time() * 1000))  # Current time in milliseconds