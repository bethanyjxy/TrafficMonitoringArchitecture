from consumer_config import initialize_consumer, commit_offsets, handle_errors
from kafka_to_hdfs import send_to_hdfs
import json

topic = 'traffic_incidents'
consumer = initialize_consumer(topic)

def handle_incidents_message(message):
    """Process traffic incidents messages."""
    data = json.loads(message.value().decode('utf-8'))
    print(f"Received incident: {data}")
    send_to_hdfs(topic, data)

if __name__ == "__main__":
    print(f"Listening to {topic} topic...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if not handle_errors(msg):
                continue
            handle_incidents_message(msg)
            commit_offsets(consumer)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
