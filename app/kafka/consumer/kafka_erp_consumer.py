from kafka_to_hdfs import send_to_hdfs
from consumer_config import initialize_consumer, commit_offsets, handle_errors, close_consumer
import json

topic = 'traffic_erp'

def handle_erp_message(message):
    """Process ERP (Electronic Road Pricing) rates messages."""
    try:
        # Decode and load message as JSON
        data = json.loads(message.value().decode('utf-8'))
        print(f"Received ERP rates: {data}")

        # Send the parsed data to HDFS
        send_to_hdfs(topic, data)
    except json.JSONDecodeError as e:
        print(f"Failed to decode message: {e}")
    except Exception as e:
        print(f"Unexpected error handling message: {e}")

def consume_erp_messages(consumer):
    """Consume ERP messages and handle them."""
    try:
        while True:
            # Poll for messages from Kafka
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if not handle_errors(msg):
                continue

            # Process the ERP message
            handle_erp_message(msg)

            # Commit the offset after processing the message
            commit_offsets(consumer)

    except KeyboardInterrupt:
        print("Consumer interrupted. Exiting...")

if __name__ == "__main__":
    consumer = initialize_consumer(topic)

    try:
        consume_erp_messages(consumer)
    finally:
        close_consumer(consumer)
