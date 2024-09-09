from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json

# Kafka configuration
kafka_topics = ['traffic_incidents', 'traffic_images', 'traffic_speedbands']  # List of topics
kafka_broker = 'localhost:9092'

# Admin client to manage Kafka topics
admin_client = AdminClient({
    'bootstrap.servers': kafka_broker
})

def create_topic(topic_name):
    """Create a Kafka topic if it does not exist."""
    # List existing topics
    topic_metadata = admin_client.list_topics(timeout=10)
    
    # Check if the topic already exists
    if topic_name in topic_metadata.topics:
        print(f"Topic '{topic_name}' already exists.")
    else:
        print(f"Creating topic '{topic_name}'...")
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        # Create the topic
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created.")

# Create the topics automatically before starting consumer
for topic in kafka_topics:
    create_topic(topic)

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': kafka_broker,       # Kafka broker address
    'group.id': 'traffic_consumer_group',    # Consumer group
    'auto.offset.reset': 'earliest'          # Start from the earliest message
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
    else:
        print(f"Received message from unknown topic '{topic}': {data}")

if __name__ == "__main__":
    print("Listening to Kafka topics...")
    try:
        while True:
            # Poll for messages (timeout 1 second)
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, no more messages
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    continue
            # Handle the received message based on its topic
            handle_message(msg.topic(), msg)
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets
        consumer.close()
