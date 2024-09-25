# kafka_config.py
from confluent_kafka import Consumer, KafkaError, KafkaException
from typing import Dict

# Kafka broker configuration
HOST: str = 'localhost'
PORT: int = 9092
TOPIC: str = 'my-topic'
CONSUMER_GROUP: str = 'traffic_consumer_group'

kafka_broker = 'localhost:9092'
# Consumer configuration
consumer_config: Dict[str, str] = {
    'bootstrap.servers': f'{HOST}:{PORT}',
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'earliest',  # Start consuming from the earliest message
    'enable.auto.commit': True,  # Automatically commit offsets
    'auto.commit.interval.ms': 5000  # Commit offsets every 5 seconds
}

def initialize_consumer(topic: str) -> Consumer:
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

def handle_errors(msg) -> bool:
    """Handle Kafka message errors."""
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition.')
            return True
        else:
            print(f"Error: {msg.error()}")
            return False
    return True

        
def close_consumer(consumer: Consumer):
    """Close Kafka Consumer connection."""
    consumer.close()
    print('Consumer connection closed.')
