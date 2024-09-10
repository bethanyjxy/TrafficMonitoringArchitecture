from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local')\
    .appName("Batch_Data") \
    .getOrCreate()
df = spark.read.format("json").load("hdfs://localhost:9000/user/hadoop/traffic_data/traffic_incidents.json")

df.show()

