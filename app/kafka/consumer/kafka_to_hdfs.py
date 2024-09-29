import os
from hdfs import InsecureClient
import json

# HDFS configuration
NAMENODE_HOST = os.environ.get('HDFS_NAMENODE_HOST', 'namenode')
NAMENODE_PORT = int(os.environ.get('HDFS_NAMENODE_PORT', 9870))
HDFS_USER = os.environ.get('HDFS_USER', 'hadoop')
HDFS_DIRECTORY = os.environ.get('HDFS_DIRECTORY', '/user/hadoop/traffic_data/')

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
        print(f"Check if HDFS is running and accessible at {NAMENODE_HOST}:{NAMENODE_PORT}. Ensure the directory '{HDFS_DIRECTORY}' exists and is writable.")

def create_directory(directory_path):
    """Ensure HDFS directory exists and has proper permissions."""
    try:
        # Check if the directory exists
        if hdfs_client.status(directory_path, strict=False):
            print(f"Directory {directory_path} already exists.")
        else:
            # Create the directory
            hdfs_client.makedirs(directory_path)
            print(f"Directory {directory_path} created successfully.")

        # Set permissions for the directory (use a more secure permission setting)
        hdfs_client.set_permission(directory_path, '755')
        print(f"Permissions for {directory_path} set to 755.")

    except Exception as e:
        print(f"Error accessing or creating directory: {e}")