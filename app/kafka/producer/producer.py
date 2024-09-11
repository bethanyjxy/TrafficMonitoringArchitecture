from confluent_kafka import Producer
import requests
import json
import time

# Your API key
api_key = '7wjYG/IcSsKUBvgpZWYdmA=='

# Kafka configuration
kafka_incidents_topic = 'traffic_incidents'
kafka_images_topic = 'traffic_images'
kafka_speedbands_topic = 'traffic_speedbands'
kafka_vms_topic = 'traffic_vms'
kafka_erp_topic = 'traffic_erp'
kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': kafka_broker
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
        print(f"Error fetching traffic incidents data: {response.status_code}")
        return []

def fetch_traffic_images():
    """ Fetch traffic images from the external API. """
    url = "https://datamall2.mytransport.sg/ltaodataservice/Traffic-Imagesv2"
    headers = {
        'AccountKey': api_key,
        'accept': 'application/json'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get('value', [])
    else:
        print(f"Error fetching traffic images data: {response.status_code}")
        return []

def fetch_traffic_speedbands():
    """ Fetch traffic speed bands from the external API. """
    url = "https://datamall2.mytransport.sg/ltaodataservice/v3/TrafficSpeedBands"
    headers = {
        'AccountKey': api_key,
        'accept': 'application/json'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get('value', [])
    else:
        print(f"Error fetching traffic speed bands data: {response.status_code}")
        return []
    
def fetch_traffic_vms():
    """ Fetch Traffic Authorities messages from the external API. """
    url = "https://datamall2.mytransport.sg/ltaodataservice/VMS"
    headers = {
        'AccountKey': api_key,
        'accept': 'application/json'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get('value', [])
    else:
        print(f"Error fetching VMS data: {response.status_code}")
        return []
    
def fetch_traffic_erp():
    """ Fetch ERP rates from the external API. """
    url = "https://datamall2.mytransport.sg/ltaodataservice/ERPRates"
    headers = {
        'AccountKey': api_key,
        'accept': 'application/json'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get('value', [])
    else:
        print(f"Error fetching ERP rates data: {response.status_code}")
        return []

def send_to_kafka(topic, data):
    """ Send fetched data to Kafka topic. """
    for record in data:
        producer.produce(
            topic,
            value=json.dumps(record).encode('utf-8'),
            callback=delivery_report
        )
    
    # Wait for all messages to be delivered
    producer.flush()

if __name__ == "__main__":
    while True:
        # Fetch and send traffic incidents
        incidents = fetch_traffic_incidents()
        send_to_kafka(kafka_incidents_topic, incidents)

        # Fetch and send traffic images
        images = fetch_traffic_images()
        send_to_kafka(kafka_images_topic, images)

        # Fetch and send traffic speed bands
        speedbands = fetch_traffic_speedbands()
        send_to_kafka(kafka_speedbands_topic, speedbands)

        # Fetch and send traffic VMS
        vms = fetch_traffic_vms()
        send_to_kafka(kafka_vms_topic, vms)

        # Fetch and send traffic ERP rates
        erp = fetch_traffic_erp()
        send_to_kafka(kafka_erp_topic, erp)

        # Wait 60 seconds before fetching again
        time.sleep(60)
