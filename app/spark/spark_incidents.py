from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExampleApp") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()
df = spark.read.json("hdfs://localhost:9870/user/hadoop/data.json")
df.show()

