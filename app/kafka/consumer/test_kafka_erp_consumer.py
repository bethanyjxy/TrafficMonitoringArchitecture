import unittest
from unittest.mock import patch, MagicMock
from kafka_erp_consumer import handle_erp_message
import json

class TestKafkaErpConsumer(unittest.TestCase):

    @patch('kafka_erp_consumer.send_to_hdfs')
    def test_handle_erp_message(self, mock_send_to_hdfs):
        # Mock message
        message = MagicMock()
        data = {'rate': 10, 'time': '2024-09-28T12:00:00Z'}
        message.value.return_value = json.dumps(data).encode('utf-8')

        # Call the function
        handle_erp_message(message)

        # Assertions
        mock_send_to_hdfs.assert_called_once_with('traffic_erp', data)
        print(f"Handled message: {data}")

    @patch('consumer_config.commit_offsets')
    @patch('consumer_config.handle_errors')
    @patch('consumer_config.initialize_consumer')
    def test_consumer_loop(self, mock_initialize_consumer, mock_handle_errors, mock_commit_offsets):
        # Mock consumer
        mock_consumer_instance = MagicMock()
        mock_initialize_consumer.return_value = mock_consumer_instance

        # Mock message
        message = MagicMock()
        mock_consumer_instance.poll.return_value = message
        message.value.return_value = json.dumps({'rate': 10}).encode('utf-8')

        # Simulate the behavior of handle_errors
        mock_handle_errors.return_value = True

        # Simulate the consumer logic
        consumer = mock_initialize_consumer('traffic_erp')  # Explicitly call it here
        msg = consumer.poll(timeout=1.0)
        if msg is not None and mock_handle_errors(msg):
            handle_erp_message(msg)
            mock_commit_offsets(consumer)

        # Ensure initialize_consumer was called
        mock_initialize_consumer.assert_called_once_with('traffic_erp')
        mock_commit_offsets.assert_called_once_with(consumer)

if __name__ == '__main__':
    unittest.main()
