from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Global variables for hostname and directory
hostname = "hdfs://namenode:8020"
directory = "/user/hadoop/historical/"

def create_spark_session(app_name):
    """Creates a Spark session."""
    logging.info(f"Creating Spark session for {app_name}")
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_csv_from_hdfs(spark, file_name, schema):
    """Reads a CSV file from HDFS and returns a DataFrame."""
    path = f"{hostname}{directory}{file_name}"
    try:
        logging.info(f"Reading CSV from {path}")
        return spark.read.csv(path, header=True, schema=schema)
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
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
            mode="append", 
            properties=postgres_properties["properties"]
        )
    except Exception as e:
        logging.error(f"Error writing to PostgreSQL: {e}")
        raise

###########################################

def read_cars_data(spark):
    """Reads and processes car data."""
    make_schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("make", StringType(), True),
        StructField("fuel_type", StringType(), True),
        StructField("number", IntegerType(), True)
    ])
    
    cc_schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("cc_rating", StringType(), True),
        StructField("number", IntegerType(), True)
    ])
    
    make_df = read_csv_from_hdfs(spark, "Cars_by_make.csv", make_schema)
    cc_df = read_csv_from_hdfs(spark, "Cars_by_cc.csv", cc_schema)

    write_to_postgres(make_df, "cars_make")
    write_to_postgres(cc_df, "cars_cc")

def read_motorcycles_data(spark):
    """Reads and processes motorcycle data."""
    make_schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("make", StringType(), True),
        StructField("fuel_type", StringType(), True),
        StructField("number", IntegerType(), True)
    ])
    
    cc_schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("cc_rating", StringType(), True),
        StructField("number", IntegerType(), True)
    ])

    make_df = read_csv_from_hdfs(spark, "MC_by_make.csv", make_schema)
    cc_df = read_csv_from_hdfs(spark, "MC_by_cc.csv", cc_schema)

    write_to_postgres(make_df, "motorcycles_make")
    write_to_postgres(cc_df, "motorcycles_cc")

def read_speed_data(spark):
    """Reads and processes road speed data."""
    speed_schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("ave_speed_expressway", FloatType(), True),
        StructField("ave_speed_arterial_roads", FloatType(), True),
        StructField("ave_daily_traffic_volume_entering_city", IntegerType(), True)
    ])
    
    speed_df = read_csv_from_hdfs(spark, "road_traffic_condition.csv", speed_schema)

    # Fix the column mismatch issue
    if "ave_speed_arterial_roads*" in speed_df.columns:
        logging.info("Renaming column 'ave_speed_arterial_roads*' to 'ave_speed_arterial_roads'")
        speed_df = speed_df.withColumnRenamed("ave_speed_arterial_roads*", "ave_speed_arterial_roads")

    write_to_postgres(speed_df, "road_traffic_condition")

def read_traffic_lights_data(spark):
    """Reads and processes traffic lights data."""
    traffic_lights_schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("traffic_lights", IntegerType(), True)
    ])

    traffic_lights_df = read_csv_from_hdfs(spark, "annual_traffic_lights.csv", traffic_lights_schema)
    write_to_postgres(traffic_lights_df, "annual_traffic_lights")

def main():
    spark = create_spark_session("HistoricalProcessing")

    try:
        read_cars_data(spark)
        read_motorcycles_data(spark)
        read_speed_data(spark)
        read_traffic_lights_data(spark)
    except Exception as e:
        logging.error(f"Error in main processing: {e}")
    finally:
        spark.stop()
        logging.info("Spark session stopped")

if __name__ == "__main__":
    main()