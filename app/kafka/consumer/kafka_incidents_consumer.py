
from kafka.consumer.kafka_to_hdfs import send_to_hdfs
from kafka.consumer.consumer_config import initialize_consumer, commit_offsets, handle_errors, close_consumer
import json

# Define the topic for traffic incident
topic = 'traffic_incidents'

def handle_incident_message(message):
    """Process traffic incident messages."""
    try:
        # Decode and load the message as JSON
        data = json.loads(message.value().decode('utf-8'))
        print(f"Received incident: {data}")

        # Send the parsed data to HDFS
        send_to_hdfs(topic, data)
        
    except json.JSONDecodeError as e:
        print(f"Failed to decode message: {e}")
    except Exception as e:
        print(f"Unexpected error handling message: {e}")

def consume_incident_messages(consumer):
    """Consume traffic incident messages and handle them."""
    try:
        while True:
            # Poll for messages from Kafka
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if not handle_errors(msg):
                continue

            # Process the traffic image message
            handle_incident_message(msg)

            # Commit the offset after processing the message
            commit_offsets(consumer)

    except KeyboardInterrupt:
        print("Consumer interrupted. Exiting...")

if __name__ == "__main__":
    consumer = initialize_consumer(topic)

    try:
        consume_incident_messages(consumer)
    finally:
        close_consumer(consumer)
