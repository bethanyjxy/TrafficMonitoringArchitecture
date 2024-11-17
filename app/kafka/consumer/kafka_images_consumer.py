from kafka_to_hdfs import send_to_hdfs
from consumer_config import initialize_consumer, commit_offsets, handle_errors
import json
import time
topic = 'traffic_images'
consumer = initialize_consumer(topic)


def consume_kafka_messages():
    """Continuously consume Kafka messages and send them to HDFS."""
    print(f"Listening to {topic} topic...")
    batch_size = 10  # Commit every 10 messages
    message_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                time.sleep(1)  # Avoid tight loops
                continue
            if not handle_errors(msg):
                continue
            handle_images_message(msg)

            # Increment the message counter
            message_count += 1

            # Commit offsets after every batch of messages
            if message_count >= batch_size:
                commit_offsets(consumer)
                message_count = 0  # Reset the counter after committing
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        
def handle_images_message(message):
    """Process traffic images messages."""
    data = json.loads(message.value().decode('utf-8'))
    print(f"Received traffic image: {data}")
    #send_to_hdfs(topic, data)  #enable this if sufficient disk memory is available 

if __name__ == "__main__":
    consume_kafka_messages()
