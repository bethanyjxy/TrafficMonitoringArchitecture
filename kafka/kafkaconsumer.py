from kafka import KafkaConsumer
import json

# Kafka configuration
kafka_topic = 'traffic_incidents'
kafka_broker = 'localhost:9092'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_broker,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='traffic_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

if __name__ == "__main__":
    print("Listening to Kafka topic...")
    for message in consumer:
        incident = message.value
        print(f"Received incident: {incident}")
