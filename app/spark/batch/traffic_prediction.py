from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, hour, current_timestamp
from pyspark.sql.types import FloatType
import logging
from batch_config import create_spark_session, get_postgres_connection

# Initialize logging
logging.basicConfig(level=logging.INFO)

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
        logging.error(f"Error reading JSON file from HDFS: {e}")
        raise

def process_speedband_data(df):
    """Processes the speedband data and calculates the average speedband per hour."""
    # Cast MinimumSpeed and MaximumSpeed to numeric types
    df = df.withColumn("MinimumSpeed", col("MinimumSpeed").cast(FloatType())) \
           .withColumn("MaximumSpeed", col("MaximumSpeed").cast(FloatType()))
    
    # Drop unnecessary columns
    columns_to_drop = ["StartLon", "StartLat", "EndLon", "EndLat"]
    df = df.drop(*[column for column in columns_to_drop if column in df.columns])

    # Ensure 'timestamp' is a valid column and calculate the average speedband per hour
    if "timestamp" in df.columns:
        avg_speedband_df = df.groupBy("RoadName", hour(col("timestamp")).alias("hour_of_day")) \
                             .agg(avg("SpeedBand").alias("average_speedband"))
    else:
        logging.error("The 'timestamp' column is missing from the data.")
        raise ValueError("The 'timestamp' column is required to calculate hourly average speedbands.")
    
    logging.info("Calculated average speedband per hour.")
    return avg_speedband_df

def write_to_postgres(df, table_name):
    """Writes the DataFrame to PostgreSQL."""
    try:
        postgres_properties = get_postgres_connection()
        logging.info(f"Writing DataFrame to PostgreSQL table: {table_name}")
        df.write.jdbc(
            url=postgres_properties["url"], 
            table=table_name, 
            mode="append", 
            properties=postgres_properties["properties"]
        )
        logging.info("Data successfully written to PostgreSQL.")
    except Exception as e:
        logging.error(f"Error writing to PostgreSQL: {e}")
        raise

def main():
    # Create Spark session
    spark = create_spark_session("HourlyAverageTrafficSpeedband")

    try:
        # Read the speedband data from HDFS
        speedband_df = read_json_from_hdfs(spark, "traffic_speedbands.json")

        # Process the speedband data
        avg_speedband_df = process_speedband_data(speedband_df)

        # Add current timestamp
        avg_speedband_df = avg_speedband_df.withColumn("recorded_at", current_timestamp())

        # Write the processed data to PostgreSQL
        write_to_postgres(avg_speedband_df, "traffic_speedband_prediction")

    except Exception as e:
        logging.error(f"Error in processing: {e}")
    finally:
        spark.stop()
        logging.info("Spark session stopped")

if __name__ == "__main__":
    main()
