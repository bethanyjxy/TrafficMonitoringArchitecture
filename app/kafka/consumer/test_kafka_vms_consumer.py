import unittest
from unittest.mock import patch, MagicMock
from kafka_vms_consumer import handle_vms_message  # Adjust this import if needed
import json

class TestKafkaVMSConsumer(unittest.TestCase):

    @patch('kafka_to_hdfs.send_to_hdfs')
    def test_handle_vms_message(self, mock_send_to_hdfs):
        # Mock message
        data = {'message': 'Traffic update'}
        message = MagicMock()
        message.value.return_value = json.dumps(data).encode('utf-8')

        # Call the function
        handle_vms_message(message)

        # Assertions
        # mock_send_to_hdfs.assert_called_once_with('traffic_vms', data)
        # print(f"Handled VMS message: {data}")

if __name__ == '__main__':
    unittest.main()
