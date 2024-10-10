# daily_incident.py
import logging
from datetime import datetime
from pyspark.sql.functions import col, to_date, current_date
from batch_config import create_spark_session, send_to_hdfs, get_postgres_connection, create_table, insert_table, generate_id


# Global variables for hostname and directory
hostname = "hdfs://namenode:8020"
directory = "/user/hadoop/traffic_data/"
          
def read_json_from_hdfs(spark, file_name):
    """Reads a JSON file from HDFS and returns a DataFrame."""
    path = f"{hostname}{directory}{file_name}"
    try:
        logging.info(f"Reading JSON from {path}")
        return spark.read.json(path) 
    except Exception as e:
        logging.error(f"Error reading JSON file: {e}")
        raise

def main():
    spark = create_spark_session("DailyIncident_BatchReport")

    # Read JSON data
    df = read_json_from_hdfs(spark, "traffic_incidents.json")
    
    # Extract timestamp from JSON data
    df = df.withColumn("timestamp", col("timestamp"))  
    
    # Filter incidents for today 
    today_incident = df.filter(to_date(col("timestamp")) == current_date())

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

    
    ####### Sending report to POSTGRESQL #######
    
    # Initialize PostgreSQL connection
    conn = get_postgres_connection()
    
    # Prepare data to insert
    data = (unique_id, report_name, total_count, current_timestamp_str)  # Data as a tuple

    # SQL queries
    create_table_query = """
    CREATE TABLE IF NOT EXISTS report_incident (
        ID VARCHAR(255) PRIMARY KEY,
        Name VARCHAR(255),
        Result INTEGER,
        Date DATE
    );
    """
    
    insert_query = """
        INSERT INTO report_incident (ID, Name, Result, Date)
        VALUES (%s, %s, %s, %s)
    """
    
    try:
        # Create the report_incident table if it does not exist
        create_table(create_table_query, conn)

        # Insert data into PostgreSQL
        insert_table(data, insert_query, conn)
    finally:
        if conn:
            conn.close()  # Ensure the connection is closed after operations

    ####### Send report to HDFS #######
    historical_data = {
        "ID": unique_id,
        "Name": report_name,
        "Result": total_count,
        "Date": current_timestamp_str
    }
    send_to_hdfs("historical_incidents", historical_data)

if __name__ == "__main__":
    main()