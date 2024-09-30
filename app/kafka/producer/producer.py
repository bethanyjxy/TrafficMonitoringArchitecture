from confluent_kafka import Producer, KafkaException, KafkaError
import requests
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time

def fetch_and_produce_data(broker):
    """Fetch data from API and produce to Kafka, while creating topics if needed."""

    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': broker
    }

    # Initialize the Kafka producer
    producer = Producer(producer_config)

    # API key for fetching traffic data
    API_KEY = '7wjYG/IcSsKUBvgpZWYdmA=='

    # Kafka topics
    KAFKA_TOPICS = {
        'incidents': 'traffic_incidents',
        'images': 'traffic_images',
        'speedbands': 'traffic_speedbands',
        'vms': 'traffic_vms',
        'erp': 'traffic_erp'
    }

    # API endpoints
    API_ENDPOINTS = {
        'incidents': "https://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents",
        'images': "https://datamall2.mytransport.sg/ltaodataservice/Traffic-Imagesv2",
        'speedbands': "https://datamall2.mytransport.sg/ltaodataservice/v3/TrafficSpeedBands",
        'vms': "https://datamall2.mytransport.sg/ltaodataservice/VMS",
        'erp': "https://datamall2.mytransport.sg/ltaodataservice/ERPRates"
    }

    # Function to create Kafka topics if they don't exist
    def create_topics():
        admin_client = AdminClient({'bootstrap.servers': broker})
        new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in KAFKA_TOPICS.values()]
        fs = admin_client.create_topics(new_topics)

        # Wait for each operation to finish
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"Topic {topic} created")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f"Topic {topic} already exists")
                else:
                    print(f"Failed to create topic {topic}: {e}")

    # Create the topics before fetching data
    create_topics()

    # Function to handle delivery reports
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    # Fetch data from the API
    def fetch_data(url, description):
        headers = {
            'AccountKey': API_KEY,
            'accept': 'application/json'
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json().get('value', [])
        else:
            print(f"Error fetching {description}: {response.status_code}")
            return []

    # Produce the data to Kafka
    def send_to_kafka(topic, data):
        for record in data:
            producer.produce(
                topic,
                value=json.dumps(record).encode('utf-8'),
                callback=delivery_report
            )
        producer.flush()

    # Fetch and send data to Kafka for each category
    for key, url in API_ENDPOINTS.items():
        data = fetch_data(url, key)
        send_to_kafka(KAFKA_TOPICS[key], data)


if __name__ == "__main__":
    # This allows the script to be run standalone for testing
    fetch_and_produce_data('kafka:9092')