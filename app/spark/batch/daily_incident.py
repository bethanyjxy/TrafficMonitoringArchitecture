import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_extract, current_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime
import time

SPARK_POSTGRES = {
    'url': 'jdbc:postgresql://localhost:5432/traffic_db',
    'properties': {
        'user': 'traffic_admin',
        'password': 'traffic_pass',
        'driver': 'org.postgresql.Driver'
    }
}

def write_to_postgres(df, table_name, postgres_url, postgres_properties):
    try:
        df.write.jdbc(url=postgres_url, table=table_name, mode="append", properties=postgres_properties)
        print(f"Successfully wrote data to {table_name}")
    except Exception as e:
        print(f"Error writing to PostgreSQL table {table_name}: {e}")
        raise

def generate_id():
    return str(int(time.time() * 1000))  # Current time in milliseconds

def main():
    # PostgreSQL connection properties
    postgres_url = SPARK_POSTGRES['url']
    postgres_properties = SPARK_POSTGRES['properties']

    # Create Spark session 
    spark = SparkSession.builder \
        .master('local[2]') \
        .appName("Incident_Batch_Process") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Path to batch data in HDFS
    batch_data_path = "hdfs://namenode:9000/user/hadoop/traffic_data/traffic_incidents.json"

    # Read JSON data
    df = spark.read.format("json").load(batch_data_path)


    # Extract date from Message
    df = df.withColumn("IncidentDate", regexp_extract(col("Message"), r"\((\d{1,2}/\d{1,2})\)", 1))
    df = df.withColumn("IncidentDate", to_date(col("IncidentDate"), "MM/dd"))

    #If want to filter based on date range 
    #start_date = '2024-09-01'
    #end_date = '2024-09-30'
    #!!! add into accident_in_range : (col("IncidentDate") >= start_date) & (col("IncidentDate") <= end_date)

    # Filter incidents of type "Accident" within the date range
    filter_accidents = df.filter(
        (col("Type") == "Accident") & 
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
    write_to_postgres(incidents_summary, "report_incident", postgres_url, postgres_properties)

if __name__ == "__main__":
    main()





