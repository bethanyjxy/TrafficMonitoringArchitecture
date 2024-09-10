# kafka_consumer.py
from confluent_kafka import Consumer, KafkaError, KafkaException
from kafka_to_hdfs import send_to_hdfs
import json

# Kafka configuration
kafka_topics = ['traffic_incidents', 'traffic_images', 'traffic_speedbands', 'traffic_vms', 'traffic_erp']  # List of topics
kafka_broker = 'localhost:9092'

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'traffic_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka consumer
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topics
consumer.subscribe(kafka_topics)

def handle_message(topic, message):
    """Process messages based on the topic."""
    data = json.loads(message.value().decode('utf-8'))

    if topic == 'traffic_incidents':
        print(f"Received incident: {data}")
    elif topic == 'traffic_images':
        print(f"Received traffic image: {data}")
    elif topic == 'traffic_speedbands':
        print(f"Received traffic speedband: {data}")
    elif topic == 'traffic_vms':
        print(f"Received VMS: {data}")
    elif topic == 'traffic_erp':
        print(f"Received ERP rates: {data}")
    else:
        print(f"Received message from unknown topic '{topic}': {data}")
    
    # Send the processed data to HDFS
    send_to_hdfs(topic, data)

def commit_offsets(consumer):
    """Commit offsets to Kafka."""
    try:
        consumer.commit(asynchronous=False)  # Commit offsets synchronously
        print("Offsets committed successfully.")
    except KafkaException as e:
        print(f"Error committing offsets: {e}")

if __name__ == "__main__":
    print("Listening to Kafka topics...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    continue
            handle_message(msg.topic(), msg)
            
            # Commit offset manually after processing
            commit_offsets(consumer)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

