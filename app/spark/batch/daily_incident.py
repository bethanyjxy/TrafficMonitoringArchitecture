from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_extract, current_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from postgresql.postgres_config import SPARK_POSTGRES
from datetime import datetime
import time
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Global variables for hostname and directory
hostname = "hdfs://namenode:8020"
directory = "/user/hadoop/traffic_data/"
    
def get_postgres_properties():
    """Returns PostgreSQL connection properties."""
    return {
        "url": SPARK_POSTGRES['url'],
        "properties": {
            "user": SPARK_POSTGRES['user'],
            "password": SPARK_POSTGRES['password'],
            "driver": SPARK_POSTGRES['driver']
        }
    }
    
def write_to_postgres(df, table_name):
    """Writes the DataFrame to PostgreSQL."""
    postgres_properties = get_postgres_properties()
    try:
        logging.info(f"Writing DataFrame to PostgreSQL table: {table_name}")
        df.write.jdbc(
            url=postgres_properties["url"], 
            table=table_name, 
            mode="overwrite", 
            properties=postgres_properties["properties"]
        )
    except Exception as e:
        logging.error(f"Error writing to PostgreSQL: {e}")
        raise
    
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

    # Read JSON data
    df = read_json_from_hdfs(spark, "traffic_incidents.json")

    # Extract date from Message and convert it to 'dd/MM' format
    df = df.withColumn("IncidentDate", regexp_extract(col("Message"), r"\((\d{1,2}/\d{1,2})\)", 1))
    df = df.withColumn("IncidentDate", to_date(col("IncidentDate"), "dd/MM"))  # Adjusted format to 'dd/MM'

    # Filter incidents within the current date 
    today_incident = df.filter((col("IncidentDate") == current_date()))

    # Calculate total count of incidents
    try:
        total_count = today_incident.count() if not today_incident.rdd.isEmpty() else 0
    except Exception as e:
        logging.error(f"Error counting today_incident: {e}")
        total_count = 0  # Default to 0 if an error occurs

    # Get current date in MMDDYY format
    current_date_str = datetime.now().strftime("%m%d%y")

    # Generate a unique ID
    unique_id = generate_id()

    # Create a DataFrame with the ID, Name, Result, and Date columns
    report_name = f"Incident Report {current_date_str}"

    # Convert current datetime to a format Spark can serialize
    current_timestamp = datetime.now()

    # Data to be added to DataFrame
    data = [(unique_id, report_name, total_count, current_timestamp)]
    schema = StructType([
        StructField("ID", StringType(), False),
        StructField("Name", StringType(), False),
        StructField("Result", IntegerType(), False),
        StructField("Date", TimestampType(), False)  # Ensure the Date column is included in schema
    ])

    logging.info("Creating DataFrame from data...")
    
    try:
        incidents_summary = spark.createDataFrame(data, schema=schema)
        logging.info("DataFrame created successfully.")
    except Exception as e:
        logging.error(f"Error creating DataFrame: {e}")

    # Write batch results to PostgreSQL
    write_to_postgres(incidents_summary, "report_incident")

if __name__ == "__main__":
    main()