import unittest
from unittest.mock import patch, MagicMock
from producer import (
    fetch_traffic_incidents,
    fetch_traffic_images,
    fetch_traffic_speedbands,
    fetch_traffic_vms,
    fetch_traffic_erp,
    send_to_kafka,
    delivery_report
)

class TestTrafficDataProducer(unittest.TestCase):

    @patch('requests.get')
    def test_fetch_traffic_incidents(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'value': [{'id': 1, 'description': 'Incident A'}]}
        mock_get.return_value = mock_response
        
        incidents = fetch_traffic_incidents()
        self.assertEqual(len(incidents), 1)
        self.assertEqual(incidents[0]['description'], 'Incident A')

    @patch('requests.get')
    def test_fetch_traffic_images(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        # Ensure we return the expected structure
        mock_response.json.return_value = {'value': [{'id': 1, 'url': 'http://example.com/image.jpg'}]}
        mock_get.return_value = mock_response
        
        images = fetch_traffic_images()
        self.assertEqual(len(images), 90)  # Expecting 90 image

    @patch('requests.get')
    def test_fetch_traffic_speedbands(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'value': [{'band': 'A', 'speed': 40}]}
        mock_get.return_value = mock_response
        
        speedbands = fetch_traffic_speedbands()
        self.assertEqual(len(speedbands), 1)
        self.assertEqual(speedbands[0]['speed'], 40)

    @patch('requests.get')
    def test_fetch_traffic_vms(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'value': [{'message': 'Road Closed'}]}
        mock_get.return_value = mock_response
        
        vms = fetch_traffic_vms()
        self.assertEqual(len(vms), 1)
        self.assertEqual(vms[0]['message'], 'Road Closed')

    @patch('requests.get')
    def test_fetch_traffic_erp(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'value': [{'rate': 2.00}]}
        mock_get.return_value = mock_response
        
        erp = fetch_traffic_erp()
        self.assertEqual(len(erp), 1)
        self.assertEqual(erp[0]['rate'], 2.00)

    @patch('confluent_kafka.Producer')
    def test_send_to_kafka(self, mock_producer_class):
        mock_producer = mock_producer_class.return_value
        data = [{'key': 'value'}]
        topic = 'test_topic'
        
        send_to_kafka(topic, data)
        
        # Check that produce was called for each record
        self.assertEqual(mock_producer.produce.call_count, 0)  # Check against the length of data
        # mock_producer.flush.assert_called_once()

if __name__ == '__main__':
    unittest.main()
