from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when, col, from_json,  regexp_extract, concat, lit, current_timestamp, regexp_replace, date_format, trim, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql import functions as F

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

# Define the mapping of CameraID to Location
camera_location_mapping = {
    "1111": "TPE(PIE) - Exit 2 to Loyang Ave",
    "1112": "TPE(PIE) - Tampines Viaduct",
    "1113": "Tanah Merah Coast Road towards Changi",
    "1701": "CTE (AYE) - Moulmein Flyover LP448F",
    "1702": "CTE (AYE) - Braddell Flyover LP274F",
    "1703": "CTE (SLE) - Blk 22 St George's Road",
    "1704": "CTE (AYE) - Entrance from Chin Swee Road",
    "1705": "CTE (AYE) - Ang Mo Kio Ave 5 Flyover",
    "1706": "CTE (AYE) - Yio Chu Kang Flyover",
    "1707": "CTE (AYE) - Bukit Merah Flyover",
    "1709": "CTE (AYE) - Exit 6 to Bukit Timah Road",
    "1711": "CTE (AYE) - Ang Mo Kio Flyover",
    "2701": "Woodlands Causeway (Towards Johor)",
    "2702": "Woodlands Checkpoint",
    "2703": "BKE (PIE) - Chantek F/O",
    "2704": "BKE (Woodlands Checkpoint) - Woodlands F/O",
    "2705": "BKE (PIE) - Dairy Farm F/O",
    "2706": "Entrance from Mandai Rd (Towards Checkpoint)",
    "2707": "Exit 5 to KJE (towards PIE)",
    "2708": "Exit 5 to KJE (Towards Checkpoint)",
    "3702": "ECP (Changi) - Entrance from PIE",
    "3704": "ECP (Changi) - Entrance from KPE",
    "3705": "ECP (AYE) - Exit 2A to Changi Coast Road",
    "3793": "ECP (Changi) - Laguna Flyover",
    "3795": "ECP (City) - Marine Parade F/O",
    "3796": "ECP (Changi) - Tanjong Katong F/O",
    "3797": "ECP (City) - Tanjung Rhu",
    "3798": "ECP (Changi) - Benjamin Sheares Bridge",
    "4701": "AYE (City) - Alexander Road Exit",
    "4702": "AYE (Jurong) - Keppel Viaduct",
    "4703": "Tuas Second Link",
    "4704": "AYE (CTE) - Lower Delta Road F/O",
    "4705": "AYE (MCE) - Entrance from Yuan Ching Rd",
    "4706": "AYE (Jurong) - NUS Sch of Computing TID",
    "4707": "AYE (MCE) - Entrance from Jln Ahmad Ibrahim",
    "4708": "AYE (CTE) - ITE College West Dover TID",
    "4709": "Clementi Ave 6 Entrance",
    "4710": "AYE(Tuas) - Pandan Garden",
    "4712": "AYE(Tuas) - Tuas Ave 8 Exit",
    "4713": "Tuas Checkpoint",
    "4714": "AYE (Tuas) - Near West Coast Walk",
    "4716": "AYE (Tuas) - Entrance from Benoi Rd",
    "4798": "Sentosa Tower 1",
    "4799": "Sentosa Tower 2",
    "5794": "PIEE (Jurong) - Bedok North",
    "5795": "PIEE (Jurong) - Eunos F/O",
    "5797": "PIEE (Jurong) - Paya Lebar F/O",
    "5798": "PIEE (Jurong) - Kallang Sims Drive Blk 62",
    "5799": "PIEE (Changi) - Woodsville F/O",
    "6701": "PIEW (Changi) - Blk 65A Jln Tenteram, Kim Keat",
    "6703": "PIEW (Changi) - Blk 173 Toa Payoh Lorong 1",
    "6704": "PIEW (Jurong) - Mt Pleasant F/O",
    "6705": "PIEW (Changi) - Adam F/O Special pole",
    "6706": "PIEW (Changi) - BKE",
    "6708": "Nanyang Flyover (Towards Changi)",
    "6710": "Entrance from Jln Anak Bukit (Towards Changi)",
    "6711": "Entrance from ECP (Towards Jurong)",
    "6712": "Exit 27 to Clementi Ave 6",
    "6713": "Entrance From Simei Ave (Towards Jurong)",
    "6714": "Exit 35 to KJE (Towards Changi)",
    "6715": "Hong Kah Flyover (Towards Jurong)",
    "6716": "AYE Flyover",
    "7791": "TPE (PIE) - Upper Changi F/O",
    "7793": "TPE(PIE) - Entrance to PIE from Tampines Ave 10",
    "7794": "TPE(SLE) - TPE Exit KPE",
    "7795": "TPE(PIE) - Entrance from Tampines FO",
    "7796": "TPE(SLE) - On rooflp of Blk 189A Rivervale Drive 9",
    "7797": "TPE(PIE) - Seletar Flyover",
    "7798": "TPE(SLE) - LP790F (On SLE Flyover)",
    "8701": "KJE (PIE) - Choa Chu Kang West Flyover",
    "8702": "KJE (BKE) - Exit To BKE",
    "8704": "KJE (BKE) - Entrance From Choa Chu Kang Dr",
    "8706": "KJE (BKE) - Tengah Flyover",
    "9701": "SLE (TPE) - Lentor F/O",
    "9702": "SLE(TPE) - Thomson Flyover",
    "9703": "SLE(Woodlands) - Woodlands South Flyover",
    "9704": "SLE(TPE) - Ulu Sembawang Flyover",
    "9705": "SLE(TPE) - Beside Slip Road From Woodland Ave 2",
    "9706": "SLE(Woodlands) - Mandai Lake Flyover"
}
    # Create a UDF to map CameraID to Location
def map_camera_id_to_location(camera_id):
    return camera_location_mapping.get(camera_id, camera_id)  # Return camera_id if location is unknown
        
map_camera_id_udf = udf(map_camera_id_to_location, StringType())
      
def process_stream(kafka_stream):
    # Define schema for each topic's data
    incidents_schema = StructType() \
        .add("Type", StringType()) \
        .add("Latitude", DoubleType()) \
        .add("Longitude", DoubleType()) \
        .add("Message", StringType())

    date_regex = r"\((\d{1,2}/\d{1,2})\)(\d{1,2}:\d{2})"
    pattern_regex = r"\(\d{1,2}/\d{1,2}\)\s*\d{1,2}:\d{2}\s*"
    time_regex = r"(\d{1,2}:\d{2})"

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
        .select(col("value.*")) \
        .withColumn("timestamp", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
        .dropDuplicates(["LinkID"]) \
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
        .withColumn("Location", map_camera_id_udf(col("CameraID"))) \
        .dropDuplicates(["CameraID"])
        #.withColumn("Location", map_camera_id_udf(col("CameraID"))) \
      
        
    vms_schema = StructType() \
        .add("EquipmentID", StringType()) \
        .add("Latitude", DoubleType()) \
        .add("Longitude", DoubleType()) \
        .add("Message", StringType())
    
    # VMS stream processing
    vms_stream = kafka_stream.filter(col("topic") == "traffic_vms") \
        .withColumn("value", from_json(col("value"), vms_schema)) \
        .select(col("value.*"))\
        .filter(col("Message") != "") \
        .withColumn("timestamp",date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss") ) \
        .dropDuplicates(["EquipmentID"])

    # erp_schema = StructType() \
    #     .add("VehicleType", StringType()) \
    #     .add("DayType", StringType()) \
    #     .add("StartTime", StringType()) \
    #     .add("EndTime", StringType()) \
    #     .add("ZoneID", StringType()) \
    #     .add("ChargeAmount", DoubleType()) \
    #     .add("EffectiveDate", StringType())
        

    # # ERP rates stream processing
    # erp_stream = kafka_stream.filter(col("topic") == "traffic_erp") \
    #     .withColumn("value", from_json(col("value"), erp_schema)) \
    #     .select(col("value.*"))\
    #     .withColumn("timestamp",date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss") )

    return incident_stream, speedbands_stream, image_stream, vms_stream  #erp_stream

def process_and_write_to_console(df, epoch_id):
    df.show(truncate=False)

def main():
    # Kafka configurations
    kafka_broker = "kafka:9092"
    kafka_topics = "traffic_incidents,traffic_images,traffic_speedbands,traffic_vms,traffic_erp"

    # Create Spark session
    spark = create_spark_session()

    # Read Kafka stream
    kafka_stream = read_kafka_stream(spark, kafka_broker, kafka_topics)

    # Process streams
    incident_stream, speedbands_stream, image_stream, vms_stream = process_stream(kafka_stream)

    # For testing purposes, print the streams to the console
    incident_query = incident_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: process_and_write_to_console(df, "incident_table")) \
        .start()

    speedbands_query = speedbands_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: process_and_write_to_console(df, "speedbands_table")) \
        .start()

    image_query = image_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: process_and_write_to_console(df, "image_table")) \
        .start()
        
    vms_query = vms_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epochId: process_and_write_to_console(df, "vms_table")) \
        .start()

    # erp_query = erp_stream.writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda df, epochId: process_and_write_to_console(df, "erp_table")) \
    #     .start()

    # Wait for the termination of the queries
    incident_query.awaitTermination()
    speedbands_query.awaitTermination()
    image_query.awaitTermination()
    vms_query.awaitTermination()
    #erp_query.awaitTermination()

if __name__ == "__main__":
    main()