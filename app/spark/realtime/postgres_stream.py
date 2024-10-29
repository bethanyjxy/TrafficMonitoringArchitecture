from pyspark.sql import SparkSession
from postgresql.postgres_config import SPARK_POSTGRES
from stream_process import create_spark_session, read_kafka_stream, process_stream

def write_to_postgres(df, table_name, postgres_url, postgres_properties):
    """Write processed DataFrame to PostgreSQL."""
    try:
        df.write.jdbc(url=postgres_url['url'], table=table_name, mode="append", properties=postgres_properties)
    except Exception as e:
        print(f"Error writing to PostgreSQL table {table_name}: {e}")

def main():
    kafka_broker = "kafka:9092"
    kafka_topics = "traffic_incidents,traffic_images,traffic_speedbands,traffic_vms"

    # PostgreSQL connection properties
    postgres_url = {"url": SPARK_POSTGRES['url']}
    postgres_properties = SPARK_POSTGRES['properties']

    # Create Spark session
    spark = create_spark_session()

    # Read Kafka stream
    kafka_stream = read_kafka_stream(spark, kafka_broker, kafka_topics)

    # Process streams
    incident_stream, speedbands_stream, image_stream, vms_stream = process_stream(kafka_stream)

    # Write streams to PostgreSQL
    queries = [
        incident_stream.writeStream.outputMode("append")
            .foreachBatch(lambda df, epochId: write_to_postgres(df, "incident_table", postgres_url, postgres_properties))
            .start(),

        speedbands_stream.writeStream.outputMode("append")
            .foreachBatch(lambda df, epochId: write_to_postgres(df, "speedbands_table", postgres_url, postgres_properties))
            .start(),

        image_stream.writeStream.outputMode("append")
            .foreachBatch(lambda df, epochId: write_to_postgres(df, "image_table", postgres_url, postgres_properties))
            .start(),

        vms_stream.writeStream.outputMode("append")
            .foreachBatch(lambda df, epochId: write_to_postgres(df, "vms_table", postgres_url, postgres_properties))
            .start()
    ]

    # Wait for all streams to terminate
    try:
        for query in queries:
            query.awaitTermination()
    except Exception as e:
        print(f"Streaming job failed: {e}")
        # Optional: Add retry logic here if needed
        raise  # Re-raise to trigger Airflow retries

if __name__ == "__main__":
    main()
