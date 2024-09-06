from kafka import KafkaProducer
import requests
import json
import time

# Your API key
api_key = '7wjYG/IcSsKUBvgpZWYdmA=='

# Kafka configuration
kafka_topic = 'traffic_incidents'
kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address

# Define your Kafka API version here
api_version = (3, 8, 0)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=api_version
)

def fetch_traffic_incidents():
    url = "https://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents"
    headers = {
        'AccountKey': api_key,
        'accept': 'application/json'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get('value', [])
    else:
        print(f"Error fetching data: {response.status_code}")
        return []

def send_to_kafka(incidents):
    for incident in incidents:
        data = {
            'Type': incident['Type'],
            'Latitude': incident['Latitude'],
            'Longitude': incident['Longitude'],
            'Message': incident['Message']
        }
        producer.send(kafka_topic, data)
        print(f"Sent to Kafka: {data}")

if __name__ == "__main__":
    while True:
        incidents = fetch_traffic_incidents()
        send_to_kafka(incidents)
        time.sleep(60)  # Fetch data every 60 seconds
