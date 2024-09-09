
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, json_tuple
from pyspark.sql.types import StringType

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")  \
    .getOrCreate()

try:
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ltaData") \
        .load()

    print("Kafka DataFrame created successfully!")
except Exception as e:
    print("Error creating Kafka DataFrame:", str(e))

# Convert the binary Kafka message value to string
raw_df = kafka_df.selectExpr("CAST(value AS STRING) as message")

raw_df.printSchema()

# Define a UDF to preprocess the JSON data dynamically
#@udf(returnType=StringType())
#def preprocess_json(json_str):
    # Preprocessing steps 
  #  return json_str 

# Apply preprocessing UDF to the raw JSON data
#preprocessed_df = raw_df.withColumn("preprocessed_data", preprocess_json(col("json_data")))



# Write the transformed messages to another Kafka topic
query = ( 
    raw_df
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "dest-topic")
    .option("checkpointLocation", "checkpoint_folder")
    .start())

query.awaitTermination()
