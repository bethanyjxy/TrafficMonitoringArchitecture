from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
    spark = SparkSession.builder \
        .appName("KafkaTest") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    return spark

def read_kafka_stream(spark, kafka_broker, kafka_topics):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topics) \
        .option("startingOffsets", "earliest") \
        .load()

def main():
    # Kafka configuration
    kafka_broker = "kafka:9092"  # Adjust to your Kafka broker address
    kafka_topics = "traffic_vms,traffic_incidents,traffic_speedbands,traffic_images"

    # Create Spark session
    spark = create_spark_session()

    # Read Kafka stream
    kafka_stream = read_kafka_stream(spark, kafka_broker, kafka_topics)

    # Select raw topic and value as string to verify data ingestion
    raw_data_stream = kafka_stream.selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)")

    # Write to console for quick verification
    query = raw_data_stream.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
