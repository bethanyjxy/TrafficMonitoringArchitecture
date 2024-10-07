from pyspark.sql import SparkSession

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("TrafficIncidentBatchProcessing") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Path to your JSON file in HDFS
    hdfs_path = "hdfs://hadoop-namenode:9000/user/hadoop/traffic_data/traffic_incidents.json"



    try:
        # Read JSON data from HDFS
        df = spark.read.json(hdfs_path)
        print("Successfully read data from HDFS!")
        
        # Show the data
        df.show(truncate=False)
    
    except Exception as e:
        print(f"Error reading data from HDFS: {e}")
    
    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    main()
