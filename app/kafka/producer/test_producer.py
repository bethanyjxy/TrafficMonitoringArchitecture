import unittest
from unittest.mock import patch, MagicMock
import json
from producer import fetch_and_produce_data

class TestKafkaProducer(unittest.TestCase):

    @patch('producer.Producer')
    @patch('producer.AdminClient')
    @patch('producer.requests.get')
    def test_fetch_and_produce_data(self, mock_get, mock_admin_client, mock_producer):
        # Mock responses for API calls
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            'value': [{'id': 1, 'data': 'test_data'}]
        }

        # Mock AdminClient
        mock_admin = MagicMock()
        mock_admin_client.return_value = mock_admin

        # Mock Producer
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        # Run the function with the mock broker
        fetch_and_produce_data('kafka:9092')

        # Check that the topics were created
        self.assertEqual(mock_admin.create_topics.call_count, 1)

        # Check that the correct API endpoints were called
        self.assertEqual(mock_get.call_count, 4)  # 4 different endpoints

        # Check that produce was called with correct parameters for all topics
        expected_topics = ['traffic_incidents', 'traffic_images', 'traffic_speedbands', 'traffic_vms']
        expected_data = json.dumps({'id': 1, 'data': 'test_data'}).encode('utf-8')

        # Verify produce calls
        for topic in expected_topics:
            calls = mock_producer_instance.produce.call_args_list
            found = any(
                call[0][0] == topic and call[1]['value'] == expected_data
                for call in calls
            )
            self.assertTrue(found, f"Produce was not called with topic {topic} and expected data.")

if __name__ == '__main__':
    unittest.main()
