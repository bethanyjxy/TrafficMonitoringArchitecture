import os
from confluent_kafka import Consumer, KafkaError, KafkaException
from typing import Dict

# Kafka broker configuration
HOST: str = os.environ.get('KAFKA_HOST', 'kafka')
PORT: int = int(os.environ.get('KAFKA_PORT', 9092))
CONSUMER_GROUP: str = os.environ.get('KAFKA_CONSUMER_GROUP', 'traffic_consumer_group')


kafka_broker = 'localhost:9092'
consumer_config = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'traffic_consumer_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,  # Automatically commit offsets
    'auto.commit.interval.ms': 5000, # Commit offsets every 5 seconds
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

        
def close_consumer(consumer: Consumer):
    """Close Kafka Consumer connection."""
    consumer.close()
    print('Consumer connection closed.')

