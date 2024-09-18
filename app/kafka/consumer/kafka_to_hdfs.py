#Send data to HDFS
from hdfs import InsecureClient
import json
import os

# HDFS configuration
hdfs_url = 'http://namenode:9870'  # Replace with your HDFS URL
hdfs_user = 'hadoop'  # Replace with your HDFS user
hdfs_directory = '/user/hadoop/traffic_data/'  # HDFS directory path

# Initialize HDFS client
hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)

def send_to_hdfs(topic, data):
    """Send data to HDFS."""
    file_path = os.path.join(hdfs_directory, f"{topic}.json")
    try:
        # Check if HDFS is reachable
        hdfs_client.status('/')  # Check if HDFS is running
        print("Connected to HDFS!")

        # Check if the file exists, if not, create it
        if not hdfs_client.status(file_path, strict=False):
            print(f"File {file_path} not found, creating it...")
            # Create the file if it does not exist
            with hdfs_client.write(file_path, encoding='utf-8') as writer:
                writer.write('')  # Create an empty file

        # Append data to the file
        with hdfs_client.write(file_path, encoding='utf-8', append=True) as writer:
            writer.write(json.dumps(data) + "\n")
        print(f"Data sent to HDFS for topic '{topic}'")
        
    except Exception as e:
        print(f"Error sending data to HDFS: {e}")
        print(f"Check if HDFS is running and accessible at {hdfs_url}. Ensure the directory '{hdfs_directory}' exists and is writable.")
