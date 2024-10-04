from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import lit

# ONCE WE GET THE CONFIGURATIONS CORRECT THIS SHOULD WORK.!! 
# Define global variables for hostname and directory
hostname = "hdfs://namenode:9000"
directory = "/user/hadoop/traffic_data/historical/"

def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.defaultFS", hostname) \
        .getOrCreate()

def read_csv_from_hdfs(spark, file_name, schema):
    """Read a CSV file from HDFS and return a DataFrame."""
    path = f"{hostname}{directory}{file_name}"
    return spark.read.csv(path, header=True, schema=schema)

def write_to_postgres(df, table_name):
    """Write the DataFrame to PostgreSQL."""
    postgres_url = "jdbc:postgresql://postgres:5432/traffic_db"
    postgres_properties = {
        "user": "traffic_admin",
        "password": "traffic_pass",
        "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(url=postgres_url, table=table_name, mode="append", properties=postgres_properties)

###########################################

def read_cars_data(spark):
    # Define schema 
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
    
    # Read csv from HDFS
    make_df = read_csv_from_hdfs(spark, "Cars_by_make.csv", make_schema)
    cc_df = read_csv_from_hdfs(spark, "Cars_by_cc.csv", cc_schema)

    # Write data to PostgreSQL
    write_to_postgres(make_df, "cars_make")
    write_to_postgres(cc_df, "cars_cc")

def read_motorcycles_data(spark):
    # Define schema 
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

    # Read csv from HDFS
    make_df = read_csv_from_hdfs(spark, "MC_by_make.csv", make_schema)
    cc_df = read_csv_from_hdfs(spark, "MC_by_cc.csv", cc_schema)

    # Write data to PostgreSQL
    write_to_postgres(make_df, "motorcycles_make")
    write_to_postgres(cc_df, "motorcycles_cc")
    
def read_speed_data(spark):
    # Define schema 
    speed_schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("ave_speed_expressway", FloatType(), True),
        StructField("ave_speed_arterial_roads", FloatType(), True),
        StructField("ave_daily_traffic_volume_entering_city", IntegerType(), True)
    ])

    # Read csv from HDFS
    speed_df = read_csv_from_hdfs(spark, "road_traffic_condition.csv", speed_schema)

    # Write data to PostgreSQL
    write_to_postgres(speed_df, "road_traffic_condition")
    
def read_traffic_lights_data(spark):
    # Define schema for traffic lights data
    traffic_lights_schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("traffic_lights", IntegerType(), True)
    ])

    # Read csv from HDFS
    traffic_lights_df = read_csv_from_hdfs(spark, "annual_traffic_lights.csv", traffic_lights_schema)

    # Write data to PostgreSQL
    write_to_postgres(traffic_lights_df, "annual_traffic_lights")

def main():
    spark = create_spark_session("HistoricalProcessing")
    read_cars_data(spark)
    read_motorcycles_data(spark)
    read_speed_data(spark)
    read_traffic_lights_data(spark) 
    spark.stop()

if __name__ == "__main__":
    main()
