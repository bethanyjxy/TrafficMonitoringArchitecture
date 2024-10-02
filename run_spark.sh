echo "Starting Kafka to Spark Streaming..."

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
  --jars /opt/spark/jars/postgresql-42.2.18.jar \
  app/spark/realtime/postgres_stream.py