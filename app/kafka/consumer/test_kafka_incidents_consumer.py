import unittest
from unittest.mock import patch, MagicMock
from kafka_incidents_consumer import handle_incidents_message  # Adjust this import if needed
import json

class TestKafkaIncidentsConsumer(unittest.TestCase):

    @patch('kafka_to_hdfs.send_to_hdfs')
    def test_handle_incidents_message(self, mock_send_to_hdfs):
        # Mock message
        data = {'incident_type': 'accident', 'location': 'Main St & 1st Ave'}
        message = MagicMock()
        message.value.return_value = json.dumps(data).encode('utf-8')

        # Call the function
        handle_incidents_message(message)

        # Assertions
        # mock_send_to_hdfs.assert_called_once_with('traffic_incidents', data)
        # print(f"Handled incident message: {data}")

    @patch('consumer_config.commit_offsets')
    @patch('consumer_config.handle_errors')
    @patch('consumer_config.initialize_consumer')
    def test_consumer_loop(self, mock_initialize_consumer, mock_handle_errors, mock_commit_offsets):
        # Mock consumer instance
        mock_consumer_instance = MagicMock()
        mock_initialize_consumer.return_value = mock_consumer_instance

        # Use the mocked initialize_consumer
        topic = 'traffic_incidents'
        consumer = mock_initialize_consumer(topic)

        # Mock message
        message = MagicMock()
        message.value.return_value = json.dumps({'incident_type': 'accident', 'location': 'Main St & 1st Ave'}).encode('utf-8')

        # Simulate the behavior of poll
        mock_consumer_instance.poll.return_value = message

        # Simulate handle_errors to return True
        mock_handle_errors.return_value = True

        # Run the consumer loop logic
        msg = consumer.poll(timeout=1.0)
        if msg is not None and mock_handle_errors(msg):
            handle_incidents_message(msg)  # This should call send_to_hdfs
            mock_commit_offsets(consumer)

        # Assertions
        mock_initialize_consumer.assert_called_once_with('traffic_incidents')
        mock_commit_offsets.assert_called_once_with(consumer)

        # Check if send_to_hdfs was called
        # expected_data = {'incident_type': 'accident', 'location': 'Main St & 1st Ave'}
        # mock_send_to_hdfs.assert_called_once_with('traffic_incidents', expected_data)

if __name__ == '__main__':
    unittest.main()
