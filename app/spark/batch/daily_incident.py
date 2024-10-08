from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_extract, current_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from postgresql.postgres_config import SPARK_POSTGRES
from datetime import datetime
import time
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Global variables for hostname and directory
hostname = "hdfs://namenode:8020"
directory = "/user/hadoop/historical/"
    
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
    return SparkSession.builder.appName(app_name).getOrCreate()
    
def generate_id():
    return str(int(time.time() * 1000))  # Current time in milliseconds

def main():
    spark = create_spark_session("DailyIncident_BatchReport")

    # Path to batch data in HDFS
    batch_data_path = "hdfs://namenode:9000/user/hadoop/traffic_data/traffic_incidents.json"

    # Read JSON data
    df = spark.read.format("json").load(batch_data_path)


    # Extract date from Message
    df = df.withColumn("IncidentDate", regexp_extract(col("Message"), r"\((\d{1,2}/\d{1,2})\)", 1))
    df = df.withColumn("IncidentDate", to_date(col("IncidentDate"), "MM/dd"))

    # Filter incidents of type "Accident" within the current date 
    filter_accidents = df.filter(
        (col("IncidentDate") == current_date())
    )

    # Calculate total count of accidents
    total_count = filter_accidents.count()

    # Get current date in MMDDYY format
    current_date_str = datetime.now().strftime("%m%d%y")

   # Generate a unique ID
    unique_id = generate_id()

    # Create a DataFrame with the ID, Name and Result columns
    report_name = f"Incident Report {current_date_str}"
    data = [(unique_id, report_name, total_count)]
    schema = StructType([
        StructField("ID", StringType(), False),
        StructField("Name", StringType(), False),
        StructField("Result", IntegerType(), False)
    ])

    incidents_summary = spark.createDataFrame(data, schema=schema)
    # Add the current timestamp as the Date column
    incidents_summary = incidents_summary.withColumn("Date", current_timestamp())


    # Write batch results to PostgreSQL
    write_to_postgres(incidents_summary, "report_incident")

if __name__ == "__main__":
    main()





