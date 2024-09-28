import unittest
from unittest.mock import patch, MagicMock
from kafka_to_hdfs import send_to_hdfs  # Adjust this import if needed
import json

class TestKafkaToHDFS(unittest.TestCase):

    @patch('kafka_to_hdfs.InsecureClient')
    def test_send_to_hdfs_create_file(self, mock_insecure_client):
        # Mock the HDFS client and its methods
        mock_client_instance = MagicMock()
        mock_insecure_client.return_value = mock_client_instance
        
        # Mocking the status method to simulate file not existing
        mock_client_instance.status.side_effect = [None, None]  # First call succeeds, second raises FileNotFoundError

        topic = 'traffic_data'
        data = {'key': 'value'}

        # Call the function
        send_to_hdfs(topic, data)

        # Assertions
        mock_client_instance.status.assert_called()  # Check status was called to verify HDFS is reachable
        mock_client_instance.write.assert_any_call(f'/user/hadoop/traffic_data/{topic}.json', encoding='utf-8')  # Check for writing the file
        mock_client_instance.write.assert_called()  # Check if write was called

        # Ensure data is written to the file
        mock_client_instance.write().write.assert_called_once_with(json.dumps(data) + "\n")

    @patch('kafka_to_hdfs.InsecureClient')
    def test_send_to_hdfs_append_to_file(self, mock_insecure_client):
        # Mock the HDFS client and its methods
        mock_client_instance = MagicMock()
        mock_insecure_client.return_value = mock_client_instance

        # Mocking the status method to simulate file existing
        mock_client_instance.status.return_value = True  # File exists

        topic = 'traffic_data'
        data = {'key': 'value'}

        # Call the function
        send_to_hdfs(topic, data)

        # Assertions
        mock_client_instance.status.assert_called()  # Check status was called
        mock_client_instance.write.assert_called()  # Check if write was called
        
        # Ensure data is written to the file
        mock_client_instance.write().write.assert_called_once_with(json.dumps(data) + "\n")

    @patch('kafka_to_hdfs.InsecureClient')
    def test_send_to_hdfs_exception_handling(self, mock_insecure_client):
        # Mock the HDFS client and its methods
        mock_client_instance = MagicMock()
        mock_insecure_client.return_value = mock_client_instance

        # Mocking the status method to always raise an exception
        mock_client_instance.status.side_effect = Exception("HDFS not reachable")

        topic = 'traffic_data'
        data = {'key': 'value'}

        # Capture the print output
        with self.assertLogs(level='ERROR') as log:
            send_to_hdfs(topic, data)

        # Assertions
        self.assertIn("Error sending data to HDFS: HDFS not reachable", log.output[0])  # Check for error log

if __name__ == '__main__':
    unittest.main()
