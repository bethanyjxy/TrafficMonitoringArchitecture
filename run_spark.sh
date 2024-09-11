# Start Kafka consumer in the background
echo "Starting Kafka Consumer..."
python3 app/kafka/consumer.py &

# Start Kafka producer in the background
echo "Starting Kafka Producer..."
python3 app/kafka/producer.py &

# # Start Spark streaming job


# in another terminal run 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 app/spark/postgres_stream.py


wait