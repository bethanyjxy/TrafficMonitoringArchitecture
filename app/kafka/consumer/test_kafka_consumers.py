import unittest
from unittest.mock import patch, MagicMock
import json
from confluent_kafka import Consumer, KafkaError
from consumer_config import initialize_consumer, commit_offsets, handle_errors, close_consumer

class TestKafkaConsumers(unittest.TestCase):
    
    @patch('consumer_config.Consumer')
    def test_initialize_consumer(self, mock_consumer):
        topic = 'test_topic'
        consumer = initialize_consumer(topic)
        mock_consumer.assert_called_once_with({
            'bootstrap.servers': 'kafka:9092',
            'group.id': f'{topic}_consumer_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
        })
        self.assertEqual(consumer, mock_consumer.return_value)
        mock_consumer.return_value.subscribe.assert_called_once_with([topic])
    
    def test_commit_offsets(self):
        mock_consumer = MagicMock()
        commit_offsets(mock_consumer)
        mock_consumer.commit.assert_called_once_with(asynchronous=False)

    def test_handle_errors_partition_eof(self):
        mock_msg = MagicMock()
        mock_msg.error.return_value.code.return_value = KafkaError._PARTITION_EOF
        self.assertTrue(handle_errors(mock_msg))

    def test_handle_errors_other(self):
        mock_msg = MagicMock()
        mock_msg.error.return_value.code.return_value = KafkaError.UNKNOWN_TOPIC_OR_PART
        with patch('builtins.print') as mock_print:
            self.assertFalse(handle_errors(mock_msg))
            mock_print.assert_called_once_with(f"Error: {mock_msg.error()}")

    @patch('consumer_config.Consumer')
    def test_close_consumer(self, mock_consumer):
        consumer = mock_consumer.return_value
        close_consumer(consumer)
        consumer.close.assert_called_once()
        print("Consumer connection closed.")  # This line will be executed during the test.

if __name__ == '__main__':
    unittest.main()
