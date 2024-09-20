from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,  regexp_extract, concat, lit, current_timestamp, regexp_replace, date_format, trim, to_timestamp
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

    date_regex = r"\((\d{1,2}/\d{1,2})\)(\d{1,2}:\d{2})"
    pattern_regex = r"\(\d{1,2}/\d{1,2}\)\d{2}:\d{2}"
    time_regex = r"(\d{2}:\d{2})" 

    # Split the stream based on topic
    incident_stream = kafka_stream.filter(col("topic") == "traffic_incidents") \
        .withColumn("value", from_json(col("value"), incidents_schema)) \
        .select(col("value.*")) \
        .withColumn("incident_date", regexp_extract(col("Message"), date_regex, 1)) \
        .withColumn("incident_time", regexp_extract(col("Message"), time_regex, 1)) \
        .withColumn("incident_message", regexp_replace(col("Message"), pattern_regex, "")) \
        .withColumn("incident_message", trim(col("incident_message"))) \
        .withColumn("timestamp",date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss") ) \
        .dropDuplicates(["Type", "Latitude", "Longitude", "Message"]) 
          
    speedbands_schema = StructType() \
        .add("LinkID", StringType()) \
        .add("RoadName", StringType()) \
        .add("RoadCategory", StringType()) \
        .add("SpeedBand", IntegerType()) \
        .add("MinimumSpeed", StringType()) \
        .add("MaximumSpeed", StringType()) \
        .add("StartLon", StringType()) \
        .add("StartLat", StringType()) \
        .add("EndLon", StringType()) \
        .add("EndLat", StringType())  
    
    speedbands_stream = kafka_stream.filter(col("topic") == "traffic_speedbands") \
        .withColumn("value", from_json(col("value"), speedbands_schema)) \
        .select(col("value.*"))\
        .withColumn("timestamp",date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss") ) \
        .dropDuplicates(["LinkID"])
        
    speedbands_stream = speedbands_stream \
        .withColumn("MinimumSpeed", col("MinimumSpeed").cast("int")) \
        .withColumn("MaximumSpeed", col("MaximumSpeed").cast("int")) \
        .withColumn("StartLon", col("StartLon").cast("double")) \
        .withColumn("StartLat", col("StartLat").cast("double")) \
        .withColumn("EndLon", col("EndLon").cast("double")) \
        .withColumn("EndLat", col("EndLat").cast("double"))
        
        
        
    images_schema = StructType() \
        .add("CameraID", StringType()) \
        .add("Latitude", DoubleType()) \
        .add("Longitude", DoubleType()) \
        .add("ImageLink", StringType())

    # Define the image stream with the additional timestamp column
    image_stream = kafka_stream.filter(col("topic") == "traffic_images") \
        .withColumn("value", from_json(col("value"), images_schema)) \
        .select(col("value.*")) \
        .withColumn("timestamp",date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss") ) \
        .dropDuplicates(["CameraID"])
        
        
    vms_schema = StructType() \
        .add("EquipmentID", StringType()) \
        .add("Latitude", DoubleType()) \
        .add("Longitude", DoubleType()) \
        .add("Message", StringType())
    
    # VMS stream processing
    vms_stream = kafka_stream.filter(col("topic") == "traffic_vms") \
        .withColumn("value", from_json(col("value"), vms_schema)) \
        .select(col("value.*"))\
        .withColumn("timestamp",date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss") ) \
        .dropDuplicates(["EquipmentID"])

    erp_schema = StructType() \
        .add("VehicleType", StringType()) \
        .add("DayType", StringType()) \
        .add("StartTime", StringType()) \
        .add("EndTime", StringType()) \
        .add("ZoneID", StringType()) \
        .add("ChargeAmount", DoubleType()) \
        .add("EffectiveDate", StringType())
        

    # ERP rates stream processing
    erp_stream = kafka_stream.filter(col("topic") == "traffic_erp") \
        .withColumn("value", from_json(col("value"), erp_schema)) \
        .select(col("value.*"))\
        .withColumn("timestamp",date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss") )

    return incident_stream, speedbands_stream, image_stream, vms_stream, erp_stream

def write_to_console(df, table_name):
    # Output the dataframe to the console for testing purposes
    df.show(truncate=False)

def main():
    # Kafka configurations
    kafka_broker = "localhost:9092"
    kafka_topics = "traffic_incidents,traffic_images,traffic_speedbands,traffic_vms,traffic_erp"

    # Create Spark session
    spark = create_spark_session()

    # Read Kafka stream
    kafka_stream = read_kafka_stream(spark, kafka_broker, kafka_topics)

    # Process streams
    incident_stream, speedbands_stream, image_stream, vms_stream, erp_stream = process_stream(kafka_stream)

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
        
    vms_query = vms_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: write_to_console(df, "vms_table")) \
        .start()

    erp_query = erp_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: write_to_console(df, "erp_table")) \
        .start()

    # Wait for the termination of the queries
    incident_query.awaitTermination()
    speedbands_query.awaitTermination()
    image_query.awaitTermination()
    vms_query.awaitTermination()
    erp_query.awaitTermination()

if __name__ == "__main__":
    main()