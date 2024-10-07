from hdfs import InsecureClient
import os
import csv

# HDFS configuration
hdfs_url = 'http://localhost:9870'  # Replace with your HDFS URL
hdfs_user = 'hadoop'  # Replace with your HDFS user
hdfs_directory = '/user/hadoop/traffic_data/historical'  # HDFS directory path

# Initialize HDFS client
hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)

def upload_csv_to_hdfs(local_csv_path):
    """Upload a local CSV file to HDFS."""
    file_name = os.path.basename(local_csv_path)
    hdfs_file_path = os.path.join(hdfs_directory, file_name)

    try:
        # Check if HDFS is reachable
        hdfs_client.status('/')  # Check if HDFS is running
        print("Connected to HDFS!")

        # Check if the file already exists in HDFS
        file_exists = hdfs_client.status(hdfs_file_path, strict=False)

        if file_exists:
            print(f"File {hdfs_file_path} already exists in HDFS. Skipping upload.")
        else:
            print(f"Uploading {local_csv_path} to HDFS at {hdfs_file_path}...")
            # Upload the entire CSV file (headers + data) to HDFS
            hdfs_client.upload(hdfs_file_path, local_csv_path)
            print(f"Uploaded {local_csv_path} to HDFS.")
    
    except Exception as e:
        print(f"Error uploading CSV file to HDFS: {e}")
        print(f"Check if HDFS is running and accessible at {hdfs_url}. Ensure the directory '{hdfs_directory}' exists and is writable.")

if __name__ == "__main__":
    # Local CSV file path (replace with relative or absolute path to your CSV file)
    local_directory = './app/historical_data'  # Assuming you're running this script inside the folder where the CSV files are
    for filename in os.listdir(local_directory):
        if filename.endswith(".csv"):
            local_csv_path = os.path.join(local_directory, filename)
            upload_csv_to_hdfs(local_csv_path)
