from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

def create_spark_session():
    # Initialize Spark session with Kafka support
    spark = SparkSession.builder \
        .appName("KafkaToSparkStream") \
        .getOrCreate()
    return spark

def read_kafka_stream(spark, kafka_broker, kafka_topics):
    # Read Kafka streams from multiple topics
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topics) \
        .option("startingOffsets", "earliest") \
        .load()

    # Select and cast the Kafka value (which is in bytes) to String
    kafka_stream = kafka_stream.selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)")
    return kafka_stream

def process_stream(kafka_stream):
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

    # Split the stream based on topic
    incident_stream = kafka_stream.filter(col("topic") == "traffic_incidents") \
        .withColumn("value", from_json(col("value"), incidents_schema)) \
        .select(col("value.*"))

    speedbands_stream = kafka_stream.filter(col("topic") == "traffic_speedbands") \
        .withColumn("value", from_json(col("value"), speedbands_schema)) \
        .select(col("value.*"))

    image_stream = kafka_stream.filter(col("topic") == "traffic_images") \
        .withColumn("value", from_json(col("value"), images_schema)) \
        .select(col("value.*"))

    return incident_stream, speedbands_stream, image_stream

def write_to_console(df, table_name):
    # Output the dataframe to the console for testing purposes
    df.show(truncate=False)

def main():
    # Kafka configurations
    kafka_broker = "localhost:9092"
    kafka_topics = "traffic_incidents,traffic_images,traffic_speedbands"

    # Create Spark session
    spark = create_spark_session()

    # Read Kafka stream
    kafka_stream = read_kafka_stream(spark, kafka_broker, kafka_topics)

    # Process streams
    incident_stream, speedbands_stream, image_stream = process_stream(kafka_stream)

    # For testing purposes, print the streams to the console
    incident_query = incident_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: write_to_console(df, "incident_table")) \
        .start()

    speedbands_query = speedbands_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: write_to_console(df, "speedbands_table")) \
        .start()

    image_query = image_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: write_to_console(df, "image_table")) \
        .start()

    # Wait for the termination of the queries
    incident_query.awaitTermination()
    speedbands_query.awaitTermination()
    image_query.awaitTermination()

if __name__ == "__main__":
    main()