# Start Kafka producer in the background
echo "Starting Kafka Producer..."
python3 app/kafka/producer/producer.py &

python3 app/kafka/consumer/kafka_to_hdfs.py &

# Start Kafka consumer in the background
echo "Starting Kafka Consumer..."
python3 app/kafka/consumer/kafka_incidents_consumer.py &
python3 app/kafka/consumer/kafka_images_consumer.py &
python3 app/kafka/consumer/kafka_speedbands_consumer.py &
python3 app/kafka/consumer/kafka_erp_consumer.py &
python3 app/kafka/consumer/kafka_vms_consumer.py &

# # Start Spark streaming job

# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
#   --jars /opt/spark/jars/postgresql-42.2.18.jar \
#   app/spark/realtime/postgres_stream.py
# in another terminal run 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 app/spark/realtime/postgres_stream.py 

