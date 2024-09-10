from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from hdfs import InsecureClient
import json
import os

# Kafka configuration
kafka_topics = ['traffic_incidents', 'traffic_images', 'traffic_speedbands']  # List of topics
kafka_broker = 'localhost:9092'

# HDFS configuration
hdfs_url = 'http://localhost:9870'  # Replace with your HDFS URL
hdfs_user = 'hadoop'  # Replace with your HDFS user
hdfs_directory = '/user/hadoop/traffic_data/'  # HDFS directory path

# Admin client to manage Kafka topics
admin_client = AdminClient({
    'bootstrap.servers': kafka_broker
})

def create_topic(topic_name):
    """Create a Kafka topic if it does not exist."""
    topic_metadata = admin_client.list_topics(timeout=10)
    
    if topic_name in topic_metadata.topics:
        print(f"Topic '{topic_name}' already exists.")
    else:
        print(f"Creating topic '{topic_name}'...")
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created.")

# Create the topics automatically before starting consumer
for topic in kafka_topics:
    create_topic(topic)

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'traffic_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka consumer
consumer = Consumer(consumer_config)

# Initialize HDFS client
hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)

# Subscribe to the Kafka topics
consumer.subscribe(kafka_topics)

def send_to_hdfs(topic, data):
    """Send data to HDFS."""
    file_path = os.path.join(hdfs_directory, f"{topic}.json")
    try:
        # Check if the file exists
        if not hdfs_client.status(file_path, strict=False):
            print(f"File {file_path} not found, creating it...")
            # Create the file if it does not exist
            with hdfs_client.write(file_path, encoding='utf-8') as writer:
                writer.write('')  # Create an empty file

        # Append data to the file
        with hdfs_client.write(file_path, encoding='utf-8', append=True) as writer:
            writer.write(json.dumps(data) + "\n")
        print(f"Data sent to HDFS for topic '{topic}'")
        
    except Exception as e:
        print(f"Error sending data to HDFS: {e}")
        print(f"Check if HDFS is running and accessible at {hdfs_url}. Ensure the directory '{hdfs_directory}' exists and is writable.")

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
    
    # Send the processed data to HDFS
    send_to_hdfs(topic, data)

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
            consumer.commit(asynchronous=False)  # Commit offsets synchronously to prevent duplicates
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()