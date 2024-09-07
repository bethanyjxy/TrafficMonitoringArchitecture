from confluent_kafka import Producer
import requests
import json
import time

# Your API key
api_key = '7wjYG/IcSsKUBvgpZWYdmA=='

# Kafka configuration
kafka_topic = 'traffic_incidents'
kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': kafka_broker  # Correct key for Kafka broker in confluent_kafka
}

# Initialize the Kafka producer
producer = Producer(producer_config)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_traffic_incidents():
    """ Fetch traffic incidents from the external API. """
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
    """ Send fetched incidents to Kafka topic. """
    for incident in incidents:
        data = {
            'Type': incident['Type'],
            'Latitude': incident['Latitude'],
            'Longitude': incident['Longitude'],
            'Message': incident['Message']
        }
        # Send the data asynchronously with a callback for delivery report
        producer.produce(
            kafka_topic,
            value=json.dumps(data).encode('utf-8'),  # Serialize the data to JSON
            callback=delivery_report
        )
    
    # Wait for all messages to be delivered
    producer.flush()

if __name__ == "__main__":
    while True:
        incidents = fetch_traffic_incidents()  # Fetch the latest traffic incidents
        send_to_kafka(incidents)  # Send the incidents to Kafka
        time.sleep(60)  # Wait 60 seconds before fetching again
