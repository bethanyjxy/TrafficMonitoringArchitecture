from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, avg, current_timestamp, date_format
from postgresql.postgres_config import SPARK_POSTGRES
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Global variables for hostname and directory
hostname = "hdfs://namenode:8020"
directory = "/user/hadoop/traffic_data/"

def create_spark_session(app_name):
    """Creates a Spark session."""
    logging.info(f"Creating Spark session for {app_name}")
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_json_from_hdfs(spark, file_name):
    """Reads a .json file from HDFS and returns a DataFrame."""
    path = f"{hostname}{directory}{file_name}"
    try:
        logging.info(f"Reading JSON from {path}")
        return spark.read.json(path)
    except Exception as e:
        logging.error(f"Error reading JSON file from HDFS: {e}")
        raise

def get_postgres_properties():
    """Returns PostgreSQL connection properties."""
    return {
        "url": "jdbc:postgresql://postgres:5432/traffic_db",
        "properties": {
            "user": "traffic_admin",
            "password": "traffic_pass",
            "driver": "org.postgresql.Driver"
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
        logging.info("Data successfully written to PostgreSQL.")
    except Exception as e:
        logging.error(f"Error writing to PostgreSQL: {e}")
        raise

def process_speedband_data(df):
    """Processes the speedband data and performs calculations."""
    # Cast MinimumSpeed and MaximumSpeed to numeric types
    df = df.withColumn("MinimumSpeed", col("MinimumSpeed").cast(FloatType())) \
           .withColumn("MaximumSpeed", col("MaximumSpeed").cast(FloatType()))
    
    # Drop unnecessary columns if they exist
    columns_to_drop = ["StartLon", "StartLat", "EndLon", "EndLat"]
    df = df.drop(*[column for column in columns_to_drop if column in df.columns])

    # Calculate average speedband per road
    avg_speed_df = df.groupBy("RoadName").agg(avg("SpeedBand").alias("average_speedband"))
    
    logging.info("Calculated average speedband per road.")
    return avg_speed_df

def read_speedband_data(spark):
    """Reads and processes speedband data from JSON."""
    speedband_df = read_json_from_hdfs(spark, "traffic_speedbands.json")

    logging.info("Schema of the DataFrame:")
    speedband_df.printSchema()

    # Process the DataFrame to calculate average speedband per road
    avg_speedband_df = process_speedband_data(speedband_df)

    # Add a current timestamp column for record-keeping
    speedband_prediction_df = avg_speedband_df.withColumn("recorded_at", current_timestamp())

    write_to_postgres(speedband_prediction_df, "traffic_speedband_predictions")

def main():
    spark = create_spark_session("TrafficSpeedbandPrediction")

    try:
        read_speedband_data(spark)
    except Exception as e:
        logging.error(f"Error in main processing: {e}")
    finally:
        spark.stop()
        logging.info("Spark session stopped")

if __name__ == "__main__":
    main()
