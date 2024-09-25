from hdfs import InsecureClient
import json
import os
import socket
from urllib3.util import connection

# HDFS configuration
NAMENODE_HOST = 'localhost'  # Update with your hostname
NAMENODE_PORT = 9870         # Port number for WebHDFS
HDFS_USER = 'hadoop'         # User for HDFS
HDFS_DIRECTORY = '/user/hadoop/traffic_data/'  # Directory in HDFS

# Build Namenode URL and HDFS WebHDFS endpoint
NAMENODE_URL = f'http://{NAMENODE_HOST}:{NAMENODE_PORT}'
HDFS_URL = f'{NAMENODE_URL}/webhdfs/v1/'

# Initialize HDFS client
hdfs_client = InsecureClient(NAMENODE_URL, user=HDFS_USER)

def send_to_hdfs(topic, data):
    """Send JSON data to HDFS."""
    file_path = os.path.join(HDFS_DIRECTORY, f"{topic}.json")
    
    try:
        # Check if HDFS is reachable
        hdfs_client.status('/')  # Check if HDFS is running
        print("Connected to HDFS!")

        # Check if the file exists, if not, create it
        if not hdfs_client.status(file_path, strict=False):
            print(f"File {file_path} not found, creating it...")
            # Create an empty file
            hdfs_client.write(file_path, data='')

        # Append data to the file
        with hdfs_client.write(file_path, encoding='utf-8', append=True) as writer:
            writer.write(json.dumps(data) + "\n")
        print(f"Data sent to HDFS for topic '{topic}'")

    except Exception as e:
        print(f"Error sending data to HDFS: {e}")
        print(f"Check if HDFS is running and accessible at {NAMENODE_URL}. Ensure the directory '{HDFS_DIRECTORY}' exists and is writable.")

def create_directory():
    """Ensure HDFS directory exists and has proper permissions."""
    try:
        # Check if the directory exists
        if hdfs_client.status(HDFS_DIRECTORY, strict=False):
            print(f"Directory {HDFS_DIRECTORY} already exists.")
        else:
            # Create the traffic_data directory
            hdfs_client.makedirs(HDFS_DIRECTORY)
            print(f"Directory {HDFS_DIRECTORY} created successfully.")

        # Optionally, set permissions for the directory (e.g., 777 for wide access)
        hdfs_client.set_permission(HDFS_DIRECTORY, '777')  
        print(f"Permissions for {HDFS_DIRECTORY} set to 777.")

    except Exception as e:
        print(f"Error accessing or creating directory: {e}")

if __name__ == "__main__":
    # Create directory if not exists
    create_directory()

    # Example of sending data to HDFS
    test_data = {"example": "This is test data"}
    send_to_hdfs('traffic_test', test_data)


