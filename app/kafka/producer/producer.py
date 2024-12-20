from confluent_kafka import Producer, KafkaException, KafkaError
import requests
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time

API_KEY = '7wjYG/IcSsKUBvgpZWYdmA=='

def create_topics(broker, kafka_topics):
    """Create Kafka topics if they don't exist."""
    admin_client = AdminClient({'bootstrap.servers': broker})
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in kafka_topics.values()]
    fs = admin_client.create_topics(new_topics)

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"Topic {topic} already exists")
            else:
                print(f"Failed to create topic {topic}: {e}")

def fetch_data(url, description):
    """Fetch data from the API with retry logic and backoff."""
    headers = {
        'AccountKey': API_KEY,
        'accept': 'application/json'
    }
    
    for attempt in range(3):  # Retry 3 times
        try:
            response = requests.get(url, headers=headers, timeout=10)  # Add a timeout
            if response.status_code == 200:
                return response.json().get('value', [])
            elif response.status_code == 429:  # Rate limit
                print(f"Rate limited for {description}. Retrying after delay.")
                time.sleep(10)  # Backoff on rate limit
            else:
                print(f"Error fetching {description}: {response.status_code}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {description}: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
    return []

def send_to_kafka(producer, topic, data):
    """Send data to Kafka topic with error handling."""
    for record in data:
        try:
            producer.produce(
                topic,
                value=json.dumps(record).encode('utf-8'),
                callback=delivery_report
            )
        except KafkaException as e:
            print(f"Failed to produce message to topic {topic}: {e}")
    producer.flush()

def delivery_report(err, msg):
    """Handle message delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err} for message {msg.value()}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_and_produce_data(broker):
    """Fetch data from API and produce to Kafka."""
    producer_config = {'bootstrap.servers': broker}
    producer = Producer(producer_config)

    KAFKA_TOPICS = {
        'incidents': 'traffic_incidents',
        'speedbands': 'traffic_speedbands',
        'vms': 'traffic_vms',
        'images': 'traffic_images',
    }

    API_ENDPOINTS = {
        'incidents': "https://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents",
        'speedbands': "https://datamall2.mytransport.sg/ltaodataservice/v3/TrafficSpeedBands",
        'vms': "https://datamall2.mytransport.sg/ltaodataservice/VMS",
    }

    # Fetch and send data to Kafka for each category
    for key, url in API_ENDPOINTS.items():
        data = fetch_data(url, key)
        send_to_kafka(producer, KAFKA_TOPICS[key], data)
        
    # Fetch and send image data specifically without API key
    image_data = fetch_image()
    if image_data:
        send_to_kafka(producer, KAFKA_TOPICS['images'], image_data)
    else:
        print("No image data fetched.")
        
    
def fetch_image():
    """Fetch image data from the API and rename 'timestamp' to 'img_timestamp'."""
    url = "https://api.data.gov.sg/v1/transport/traffic-images"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        # Check if there are items in the data
        if 'items' in data and len(data['items']) > 0:
            image_data = []
            
            # Rename 'timestamp' to 'img_timestamp' in each item
            for item in data['items']:
                img_timestamp = item.pop('timestamp', None)  # Remove original 'timestamp'
                
                for camera in item.get('cameras', []):
                    camera_id = camera.get('camera_id')
                    image_url = camera.get('image')
                    latitude = camera['location']['latitude']
                    longitude = camera['location']['longitude']
                    
                    # Add the image data along with the renamed 'img_timestamp'
                    image_data.append({
                        'camera_id': camera_id,
                        'image_url': image_url,
                        'latitude': latitude,
                        'longitude': longitude,
                        'img_timestamp': img_timestamp  # Renamed timestamp
                    })

            return image_data  # Return the image data with renamed timestamps
    
    return []  # Return empty if no data is fetched

if __name__ == "__main__":
    broker = 'kafka:9092'
    create_topics(broker, {
        'incidents': 'traffic_incidents',
        'speedbands': 'traffic_speedbands',
        'vms': 'traffic_vms',
        'images': 'traffic_images',
    })

    while True:
        try:
            fetch_and_produce_data(broker)
            time.sleep(300)  # Sleep for 5 minutes between API fetches
        except Exception as e:
            print(f"Error occurred: {e}")
            time.sleep(60)  # Sleep for 1 minute before retrying
