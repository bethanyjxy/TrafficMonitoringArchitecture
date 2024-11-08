import unittest
from unittest.mock import patch, MagicMock
import json
from producer import create_topics, fetch_data, send_to_kafka, fetch_and_produce_data, fetch_image  # Update with your actual module name

class TestKafkaFunctions(unittest.TestCase):

    @patch('producer.AdminClient')
    def test_create_topics(self, mock_admin_client):
        # Arrange
        mock_topic = MagicMock()
        mock_admin_client.return_value.create_topics.return_value = { 'test_topic': mock_topic }
        kafka_topics = {'test_topic': 'test_topic'}
        
        # Act
        create_topics('localhost:9092', kafka_topics)

        # Assert
        mock_admin_client.assert_called_once_with({'bootstrap.servers': 'localhost:9092'})
        mock_topic.result.assert_called_once()

    @patch('producer.requests.get')
    def test_fetch_data_success(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'value': [{'test': 'data'}]}
        mock_get.return_value = mock_response
        url = 'http://example.com'
        
        # Act
        result = fetch_data(url, 'test_description')
        
        # Assert
        self.assertEqual(result, [{'test': 'data'}])
        mock_get.assert_called_once_with(url, headers={'AccountKey': '7wjYG/IcSsKUBvgpZWYdmA==', 'accept': 'application/json'}, timeout=10)

    @patch('producer.Producer')
    @patch('producer.delivery_report')
    def test_send_to_kafka(self, mock_delivery_report, mock_producer):
        # Arrange
        mock_producer_instance = mock_producer.return_value
        data = [{'key': 'value'}]
        topic = 'test_topic'
        
        # Act
        send_to_kafka(mock_producer_instance, topic, data)

        # Assert
        mock_producer_instance.produce.assert_called_once_with(
            topic,
            value=json.dumps({'key': 'value'}).encode('utf-8'),
            callback=mock_delivery_report
        )
        mock_producer_instance.flush.assert_called_once()

    @patch('producer.requests.get')
    def test_fetch_image_success(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'items': [
                {
                    'timestamp': '2024-11-01T12:00:00',
                    'cameras': [
                        {
                            'camera_id': '1',
                            'image': 'http://example.com/image.jpg',
                            'location': {'latitude': 1.234, 'longitude': 5.678}
                        }
                    ]
                }
            ]
        }
        mock_get.return_value = mock_response

        # Act
        result = fetch_image()
        
        # Assert
        expected_result = [{
            'camera_id': '1',
            'image_url': 'http://example.com/image.jpg',
            'latitude': 1.234,
            'longitude': 5.678,
            'img_timestamp': '2024-11-01T12:00:00'
        }]
        self.assertEqual(result, expected_result)

    @patch('producer.fetch_data')
    @patch('producer.send_to_kafka')
    @patch('producer.Producer')
    def test_fetch_and_produce_data(self, mock_producer, mock_send_to_kafka, mock_fetch_data):
        # Arrange
        mock_producer_instance = mock_producer.return_value
        mock_fetch_data.side_effect = [
            [{'incident': 'data'}],
            [{'speedband': 'data'}],
            [{'vms': 'data'}]
        ]
        
        # Act
        fetch_and_produce_data('localhost:9092')

        # Assert
        mock_send_to_kafka.assert_any_call(mock_producer_instance, 'traffic_incidents', [{'incident': 'data'}])
        mock_send_to_kafka.assert_any_call(mock_producer_instance, 'traffic_speedbands', [{'speedband': 'data'}])
        mock_send_to_kafka.assert_any_call(mock_producer_instance, 'traffic_vms', [{'vms': 'data'}])

if __name__ == '__main__':
    unittest.main()
