from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import requests
import json
import time

# Kafka configuration
kafka_topic = 'traffic_incidents'
kafka_broker = 'localhost:9092'


# Admin client to manage Kafka topics
admin_client = AdminClient({
    'bootstrap.servers': kafka_broker
})

def create_topic(topic_name):
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

# Create the topic automatically before starting consumer
create_topic(kafka_topic)


# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': kafka_broker,  # Kafka broker address
    'group.id': 'traffic_consumer_group',  # Consumer group
    'auto.offset.reset': 'earliest'  # Start from the earliest message
}

# Initialize Kafka consumer
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
consumer.subscribe([kafka_topic])

if __name__ == "__main__":
    print("Listening to Kafka topic...")
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
            # Deserialize the message value (assuming it's JSON)
            incident = json.loads(msg.value().decode('utf-8'))
            print(f"Received incident: {incident}")
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets
        consumer.close()