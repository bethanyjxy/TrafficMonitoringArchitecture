import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaSparkStreaming {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session with Kafka support
    val spark = SparkSession.builder()
      .appName("KafkaSparkStreaming")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2")
      .getOrCreate()

    try {
      // Create a DataFrame that reads from the Kafka topic "ltaData"
      val kafkaDF = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "ltaData")
        .option("startingOffsets", "earliest")
        .load()

      println("Kafka DataFrame created successfully!")

      // Convert the binary Kafka message value to string
      val rawDF = kafkaDF.selectExpr("CAST(value AS STRING) as message")

      rawDF.printSchema()

      // Example of writing data back to Kafka
      val query = rawDF
        .selectExpr("CAST(message AS STRING) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "dest-topic")
        .option("checkpointLocation", "checkpoint_folder")
        .start()

      query.awaitTermination()

    } catch {
      case e: Exception =>
        println("Error creating Kafka DataFrame: " + e.getMessage)
    }
  }
}
