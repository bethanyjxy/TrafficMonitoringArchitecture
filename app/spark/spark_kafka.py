from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

# Set Spark configurations
os.environ['SPARK_HOME'] = '/home/bethany/Software/spark-3.5.2-bin-hadoop3'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/bethany/Software/spark-3.5.2-bin-hadoop3/jars/spark-sql-kafka-0-10_2.12-3.5.2.jar pyspark-shell'

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToPostgreSQL") \
    .getOrCreate()

# Define the schema for the Kafka messages
schema = StructType([
    StructField("Type", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Message", StringType(), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_data") \
    .load()

# Convert Kafka value to JSON format and apply the schema
json_df = df.selectExpr("CAST(value AS STRING) as json").select(from_json(col("json"), schema).alias("data")).select("data.*")

# Process data (optional): You can perform any transformation you need here
processed_df = json_df.filter(json_df.Type.isNotNull())  # Example: filter out null values

# Write the processed data to PostgreSQL
def write_to_postgresql(df, epoch_id):
    # Replace with your PostgreSQL credentials
    url = "jdbc:postgresql://localhost:5432/trafficmonitoring"
    properties = {
        "user": "bethany",
        "password": "bethany",
        "driver": "org.postgresql.Driver"
    }

    # Write DataFrame to PostgreSQL
    df.write.jdbc(url=url, table="traffic_incidents", mode="append", properties=properties)

processed_df.writeStream \
    .foreachBatch(write_to_postgresql) \
    .start() \
    .awaitTermination()


