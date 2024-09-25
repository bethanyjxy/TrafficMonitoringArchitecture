from confluent_kafka import Producer
import requests
import json
import time

# API key for fetching traffic data
API_KEY = '7wjYG/IcSsKUBvgpZWYdmA=='

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Replace with your Kafka broker address
KAFKA_TOPICS = {
    'incidents': 'traffic_incidents',
    'images': 'traffic_images',
    'speedbands': 'traffic_speedbands',
    'vms': 'traffic_vms',
    'erp': 'traffic_erp'
}

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': KAFKA_BROKER
}

# Initialize the Kafka producer
producer = Producer(producer_config)

def delivery_report(err, msg):
    """Callback function to handle message delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_data(url, description):
    """Fetch data from the API."""
    payload = {}
    headers = {
        'AccountKey': API_KEY,
        'accept': 'application/json'
    }
    
    response = requests.request("GET", url, headers=headers, data=payload)
    
    if response.status_code == 200:
        return response.json().get('value', [])
    else:
        print(f"Error fetching {description}: {response.status_code}")
        return []

def send_to_kafka(topic, data):
    """Send data to a Kafka topic."""
    for record in data:
        producer.produce(
            topic,
            value=json.dumps(record).encode('utf-8'),
            callback=delivery_report
        )
    # Wait for all messages to be delivered
    producer.flush()

def main():
    while True:
        # API endpoints
        API_ENDPOINTS = {
            'incidents': "https://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents",
            'images': "https://datamall2.mytransport.sg/ltaodataservice/Traffic-Imagesv2",
            'speedbands': "https://datamall2.mytransport.sg/ltaodataservice/v3/TrafficSpeedBands",
            'vms': "https://datamall2.mytransport.sg/ltaodataservice/VMS",
            'erp': "https://datamall2.mytransport.sg/ltaodataservice/ERPRates"
        }

        # Fetch and send data to Kafka for each category
        for key, url in API_ENDPOINTS.items():
            data = fetch_data(url, key)
            send_to_kafka(KAFKA_TOPICS[key], data)
        
        # Wait for 60 seconds before fetching again
        time.sleep(60)

if __name__ == "__main__":
    main()
