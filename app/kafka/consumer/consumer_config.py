# kafka_config.py
from confluent_kafka import Consumer, KafkaError, KafkaException

kafka_broker = 'localhost:9092'
consumer_config = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'traffic_consumer_group',
    'auto.offset.reset': 'earliest'
}

def initialize_consumer(topic):
    """Initialize Kafka consumer for a specific topic."""
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    return consumer

def commit_offsets(consumer):
    """Commit offsets to Kafka."""
    try:
        consumer.commit(asynchronous=False)  # Commit offsets synchronously
        print("Offsets committed successfully.")
    except KafkaException as e:
        print(f"Error committing offsets: {e}")

def handle_errors(msg):
    """Handle Kafka message errors."""
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            return True
        else:
            print(f"Error: {msg.error()}")
            return False
    return True
