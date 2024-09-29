from kafka.consumer.kafka_to_hdfs import send_to_hdfs
from kafka.consumer.consumer_config import initialize_consumer, commit_offsets, handle_errors, close_consumer
import json

topic = 'traffic_erp'
consumer = initialize_consumer(topic)

def handle_erp_message(message):
    """Process ERP (Electronic Road Pricing) rates messages."""
    data = json.loads(message.value().decode('utf-8'))
    print(f"Received ERP rates: {data}")
    send_to_hdfs(topic, data)

if __name__ == "__main__":
    print(f"Listening to {topic} topic...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print("No message received")
                continue
            if not handle_errors(msg):
                continue
            handle_erp_message(msg)
            commit_offsets(consumer)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()