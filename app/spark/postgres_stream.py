from pyspark.sql import SparkSession
from stream_process import create_spark_session, read_kafka_stream, process_stream

def write_to_postgres(df, table_name, postgres_url, postgres_properties):
    try:
        df.write.jdbc(url=postgres_url, table=table_name, mode="append", properties=postgres_properties)
    except Exception as e:
        print(f"Error writing to PostgreSQL table {table_name}: {e}")
        raise


def main():
    # Kafka configurations
    kafka_broker = "localhost:9092"
    kafka_topics = "traffic_incidents,traffic_images,traffic_speedbands"

    
    # PostgreSQL connection properties
    postgres_url = "jdbc:postgresql://localhost:5432/trafficmonitoring"
    postgres_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    # Create Spark session
    spark = create_spark_session()

    # Read Kafka stream
    kafka_stream = read_kafka_stream(spark, kafka_broker, kafka_topics)

    # Process streams
    incident_stream, speedbands_stream, image_stream = process_stream(kafka_stream)

    # Write streams to PostgreSQL
    incident_query = incident_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: write_to_postgres(df, "incident_table", postgres_url, postgres_properties)) \
        .start()

    speedbands_query = speedbands_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: write_to_postgres(df, "speedbands_table", postgres_url, postgres_properties)) \
        .start()

    image_query = image_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: write_to_postgres(df, "image_table", postgres_url, postgres_properties)) \
        .start()

    # Wait for the termination of the queries
    incident_query.awaitTermination()
    speedbands_query.awaitTermination()
    image_query.awaitTermination()

if __name__ == "__main__":
    main()