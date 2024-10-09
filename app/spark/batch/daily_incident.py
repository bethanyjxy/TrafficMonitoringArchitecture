import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_extract, current_date
from datetime import datetime
import time
from postgresql.postgres_config import POSTGRES_DB
from pyspark.sql import functions as F

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Global variables for hostname and directory
hostname = "hdfs://namenode:8020"
directory = "/user/hadoop/traffic_data/"

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

def create_report_incident_table():
    """Creates the report_incident table if it does not exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS report_incident (
        ID VARCHAR(255) PRIMARY KEY,
        Name VARCHAR(255),
        Result INTEGER,
        Date DATE
    );
    """
    conn = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        logging.info("Table 'report_incident' created or already exists.")
    except Exception as e:
        logging.error(f"Error creating table in PostgreSQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            
def insert_into_postgres(data):
    """Inserts data into PostgreSQL."""
    insert_query = """
        INSERT INTO report_incident (ID, Name, Result, Date)
        VALUES (%s, %s, %s, %s)
    """
    conn = None
    try:
        conn = get_postgres_connection()
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
    finally:
        if conn:
            conn.close()

def create_spark_session(app_name):
    """Creates a Spark session."""
    logging.info(f"Creating Spark session for {app_name}")
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

def read_json_from_hdfs(spark, file_name):
    """Reads a JSON file from HDFS and returns a DataFrame."""
    path = f"{hostname}{directory}{file_name}"
    try:
        logging.info(f"Reading JSON from {path}")
        return spark.read.json(path) 
    except Exception as e:
        logging.error(f"Error reading JSON file: {e}")
        raise

def generate_id():
    return str(int(time.time() * 1000))  # Current time in milliseconds

def main():
    spark = create_spark_session("DailyIncident_BatchReport")
    # Create the report_incident table if it does not exist
    create_report_incident_table()

    # Read JSON data
    df = read_json_from_hdfs(spark, "traffic_incidents.json")


    # Extract date from Message
    df = df.withColumn("IncidentDate", regexp_extract(col("Message"), r"\((\d{1,2}/\d{1,2})\)", 1))

    # Get the current year
    current_year = datetime.now().year
    
    # Create full date strings in the format "yyyy-MM-dd"
    df = df.withColumn("IncidentDate",
                    F.concat_ws("-", F.lit(current_year), 
                                F.split(col("IncidentDate"), "/")[1],  # Day
                                F.split(col("IncidentDate"), "/")[0]))  # Month

    # Convert to date type
    df = df.withColumn("IncidentDate", to_date(col("IncidentDate"), "yyyy-MM-dd"))

    # Filter incidents for today (excluding year)
    today_incident = df.filter((col("IncidentDate") == current_date()))


    # Calculate total count of incidents
    try:
        total_count = today_incident.count()
        logging.info(f"Total count of today's incidents: {total_count}")
    except Exception as e:
        logging.error(f"Error counting today_incident: {e}")
        total_count = 0  # Default to 0 if an error occurs

    # Get current date in MMDDYY format
    current_date_str = datetime.now().strftime("%d%m%y")

    # Generate a unique ID
    unique_id = generate_id()

    # Prepare data to insert
    report_name = f"Incident Report {current_date_str}"

    # Convert current datetime to a string
    current_timestamp_str = datetime.now().strftime("%Y-%m-%d")

    # Data as a tuple
    data = (unique_id, report_name, total_count, current_timestamp_str)

    # Insert data into PostgreSQL
    insert_into_postgres(data)

if __name__ == "__main__":
    main()
