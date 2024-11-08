import unittest
from unittest.mock import patch, MagicMock
import json
import kafka_erp_consumer
import kafka_images_consumer
import kafka_incidents_consumer
import kafka_speedbands_consumer
import kafka_vms_consumer

class TestKafkaConsumers(unittest.TestCase):

    @patch('kafka_erp_consumer.send_to_hdfs')
    @patch('kafka_erp_consumer.initialize_consumer')
    @patch('kafka_erp_consumer.handle_errors')
    def test_kafka_erp_consumer(self, mock_handle_errors, mock_initialize_consumer, mock_send_to_hdfs):
        topic = 'traffic_erp'
        message = MagicMock()
        message.value.return_value = json.dumps({"rate": 10}).encode('utf-8')
        
        mock_initialize_consumer.return_value.poll.return_value = message
        mock_handle_errors.return_value = True
        
        kafka_erp_consumer.handle_erp_message(message)
        
        mock_send_to_hdfs.assert_called_once_with(topic, {"rate": 10})

    @patch('kafka_images_consumer.send_to_hdfs')
    @patch('kafka_images_consumer.initialize_consumer')
    @patch('kafka_images_consumer.handle_errors')
    def test_kafka_images_consumer(self, mock_handle_errors, mock_initialize_consumer, mock_send_to_hdfs):
        topic = 'traffic_images'
        message = MagicMock()
        message.value.return_value = json.dumps({"image_id": "12345"}).encode('utf-8')
        
        mock_initialize_consumer.return_value.poll.return_value = message
        mock_handle_errors.return_value = True
        
        kafka_images_consumer.handle_images_message(message)
        
        mock_send_to_hdfs.assert_called_once_with(topic, {"image_id": "12345"})

    @patch('kafka_incidents_consumer.send_to_hdfs')
    @patch('kafka_incidents_consumer.initialize_consumer')
    @patch('kafka_incidents_consumer.handle_errors')
    def test_kafka_incidents_consumer(self, mock_handle_errors, mock_initialize_consumer, mock_send_to_hdfs):
        topic = 'traffic_incidents'
        message = MagicMock()
        message.value.return_value = json.dumps({"incident": "accident"}).encode('utf-8')
        
        mock_initialize_consumer.return_value.poll.return_value = message
        mock_handle_errors.return_value = True
        
        kafka_incidents_consumer.handle_incidents_message(message)
        
        mock_send_to_hdfs.assert_called_once_with(topic, {"incident": "accident"})

    @patch('kafka_speedbands_consumer.send_to_hdfs')
    @patch('kafka_speedbands_consumer.initialize_consumer')
    @patch('kafka_speedbands_consumer.handle_errors')
    def test_kafka_speedbands_consumer(self, mock_handle_errors, mock_initialize_consumer, mock_send_to_hdfs):
        topic = 'traffic_speedbands'
        message = MagicMock()
        message.value.return_value = json.dumps({"speed": 80}).encode('utf-8')
        
        mock_initialize_consumer.return_value.poll.return_value = message
        mock_handle_errors.return_value = True
        
        kafka_speedbands_consumer.handle_speedbands_message(message)
        
        mock_send_to_hdfs.assert_called_once_with(topic, {"speed": 80})

    @patch('kafka_vms_consumer.send_to_hdfs')
    @patch('kafka_vms_consumer.initialize_consumer')
    @patch('kafka_vms_consumer.handle_errors')
    def test_kafka_vms_consumer(self, mock_handle_errors, mock_initialize_consumer, mock_send_to_hdfs):
        topic = 'traffic_vms'
        message = MagicMock()
        message.value.return_value = json.dumps({"message": "VMS Alert"}).encode('utf-8')
        
        mock_initialize_consumer.return_value.poll.return_value = message
        mock_handle_errors.return_value = True
        
        kafka_vms_consumer.handle_vms_message(message)
        
        mock_send_to_hdfs.assert_called_once_with(topic, {"message": "VMS Alert"})

if __name__ == '__main__':
    unittest.main()
