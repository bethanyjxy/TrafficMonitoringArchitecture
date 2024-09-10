
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, json_tuple
from pyspark.sql.types import StructType, StringType, DoubleType

# Initialize Spark session with Kafka support #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")  
spark = SparkSession.builder \
    .appName("KafkaToSparkStream") \
    .getOrCreate()

# Kafka topic and broker configurations
kafka_topic = "traffic_incidents"
kafka_broker = "localhost:9092"

# Define the schema of the incoming JSON data
schema = StructType() \
    .add("Type", StringType()) \
    .add("Latitude", DoubleType()) \
    .add("Longitude", DoubleType()) \
    .add("Message", StringType())

# Read the stream from the Kafka topic
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# The Kafka message value is in bytes, so we cast it to a string
traffic_data = kafka_stream.selectExpr("CAST(value AS STRING)")

# Parse the JSON message
parsed_data = traffic_data.withColumn("value", from_json(col("value"), schema)) \
    .select(col("value.*"))

# Perform stream processing on the data (e.g., filtering, aggregations)
# For demonstration, we'll just filter for "Accidents"
filtered_data = parsed_data.filter(parsed_data.Type == "Accident")

# Output the results to the console (can be redirected to another sink like a DB)
query = filtered_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination of the streaming query
query.awaitTermination()