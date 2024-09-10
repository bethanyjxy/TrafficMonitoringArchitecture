
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, json_tuple
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Initialize Spark session with Kafka support #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")  
spark = SparkSession.builder \
    .appName("KafkaToSparkStream") \
    .getOrCreate()

# Kafka topic and broker configurations
kafka_incidents_topic = 'traffic_incidents'
kafka_images_topic = 'traffic_images'
kafka_speedbands_topic = 'traffic_speedbands'
kafka_broker = "localhost:9092"

# Define schema for each topic's data
incidents_schema = StructType() \
    .add("Type", StringType()) \
    .add("Latitude", DoubleType()) \
    .add("Longitude", DoubleType()) \
    .add("Message", StringType())

speedbands_schema = StructType() \
    .add("LinkID", StringType()) \
    .add("RoadName", StringType()) \
    .add("RoadCategory", StringType()) \
    .add("SpeedBand", IntegerType()) \
    .add("MinimumSpeed", IntegerType()) \
    .add("MaximumSpeed", IntegerType()) \
    .add("StartLon", DoubleType())

images_schema = StructType() \
    .add("CameraID", StringType()) \
    .add("Latitude", DoubleType()) \
    .add("Longitude", DoubleType()) \
    .add("ImageLink", StringType())

# Read Kafka streams from multiple topics
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", f"{kafka_incidents_topic},{kafka_images_topic},{kafka_speedbands_topic}") \
    .option("startingOffsets", "earliest") \
    .load()

# Select and cast the Kafka value (which is in bytes) to String
kafka_stream = kafka_stream.selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)")

# Split the stream based on topic

# Incident stream processing
incident_stream = kafka_stream.filter(col("topic") == kafka_incidents_topic) \
    .withColumn("value", from_json(col("value"), incidents_schema)) \
    .select(col("value.*"))

# Speedbands stream processing
speedbands_stream = kafka_stream.filter(col("topic") == kafka_speedbands_topic) \
    .withColumn("value", from_json(col("value"), speedbands_schema)) \
    .select(col("value.*"))

# Image stream processing
image_stream = kafka_stream.filter(col("topic") == kafka_images_topic) \
    .withColumn("value", from_json(col("value"), images_schema)) \
    .select(col("value.*"))

# Output the processed streams (in this example, to console)
incident_query = incident_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

speedbands_query = speedbands_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

image_query = image_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for termination
incident_query.awaitTermination()
speedbands_query.awaitTermination()
image_query.awaitTermination()