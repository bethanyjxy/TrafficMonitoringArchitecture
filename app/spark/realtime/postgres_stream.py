from pyspark.sql import SparkSession
from postgresql.postgres_config import SPARK_POSTGRES
from stream_process import create_spark_session, read_kafka_stream, process_stream

def write_to_postgres(df, table_name, postgres_url, postgres_properties):
    try:
        df.write.jdbc(url=postgres_url['url'], table=table_name, mode="append", properties=postgres_properties)
    except Exception as e:
        print(f"Error writing to PostgreSQL table {table_name}: {e}")
        raise


def main():
    # Kafka configurations
    kafka_broker = "kafka:9092"
    kafka_topics = "traffic_incidents,traffic_images,traffic_speedbands,traffic_vms"


    # PostgreSQL connection properties
    postgres_url = {"url" : SPARK_POSTGRES['url']}
    postgres_properties = SPARK_POSTGRES['properties']

    # Create Spark session
    spark = create_spark_session()

    # Read Kafka stream
    kafka_stream = read_kafka_stream(spark, kafka_broker, kafka_topics)

    # Process streams
    incident_stream, speedbands_stream, image_stream, vms_stream = process_stream(kafka_stream)

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
        
    vms_query = vms_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: write_to_postgres(df, "vms_table", postgres_url, postgres_properties)) \
        .start()
        
    # Wait for the termination of the queries
    incident_query.awaitTermination()
    speedbands_query.awaitTermination()
    image_query.awaitTermination()
    vms_query.awaitTermination()

if __name__ == "__main__":
    main()
