import unittest
from unittest.mock import patch, MagicMock
import json
import producer  # Adjust the import based on your file structure

class TestKafkaProducer(unittest.TestCase):

    @patch('producer.AdminClient')
    def test_create_topics(self, mock_AdminClient):
        mock_admin_client = MagicMock()
        mock_AdminClient.return_value = mock_admin_client
        mock_admin_client.create_topics.return_value = {}

        kafka_topics = {
            'incidents': 'traffic_incidents',
            'images': 'traffic_images',
            'speedbands': 'traffic_speedbands',
            'vms': 'traffic_vms',
        }

        producer.create_topics('kafka:9092', kafka_topics)

        # Check that create_topics was called with the correct arguments
        mock_admin_client.create_topics.assert_called_once()
        new_topics = mock_admin_client.create_topics.call_args[0][0]
        self.assertEqual(len(new_topics), len(kafka_topics))

    @patch('producer.requests.get')
    def test_fetch_data_success(self, mock_get):
        url = 'https://example.com/api'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'value': [{'data': 'example_data'}]}
        mock_get.return_value = mock_response

        result = producer.fetch_data(url, 'test_description')
        self.assertEqual(result, [{'data': 'example_data'}])

    @patch('producer.requests.get')
    def test_fetch_data_rate_limit(self, mock_get):
        url = 'https://example.com/api'
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_get.return_value = mock_response

        result = producer.fetch_data(url, 'test_description')
        self.assertEqual(result, [])

    @patch('producer.requests.get')
    def test_fetch_data_error(self, mock_get):
        url = 'https://example.com/api'
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = producer.fetch_data(url, 'test_description')
        self.assertEqual(result, [])

    @patch('producer.Producer')
    def test_send_to_kafka(self, mock_Producer):
        mock_producer = MagicMock()
        mock_Producer.return_value = mock_producer
        topic = 'test_topic'
        data = [{'key': 'value'}]

        producer.send_to_kafka(mock_producer, topic, data)

        # Check that produce was called for each record
        self.assertEqual(mock_producer.produce.call_count, len(data))

    @patch('producer.Producer')
    @patch('producer.delivery_report')
    def test_delivery_report_success(self, mock_delivery_report, mock_Producer):
        mock_producer = MagicMock()
        mock_Producer.return_value = mock_producer
        message = MagicMock()
        message.value.return_value = json.dumps({'key': 'value'})

        producer.delivery_report(None, message)

        mock_delivery_report.assert_called_once_with(None, message)

    @patch('producer.time.sleep')
    @patch('producer.fetch_data')
    @patch('producer.send_to_kafka')
    @patch('producer.Producer')
    def test_fetch_and_produce_data(self, mock_Producer, mock_send_to_kafka, mock_fetch_data, mock_sleep):
        mock_producer = MagicMock()
        mock_Producer.return_value = mock_producer

        # Mock the API responses
        mock_fetch_data.return_value = [{'data': 'example_data'}]

        producer.fetch_and_produce_data('kafka:9092')

        # Ensure data was fetched and sent to Kafka
        mock_send_to_kafka.assert_called_once()

if __name__ == '__main__':
    unittest.main()
