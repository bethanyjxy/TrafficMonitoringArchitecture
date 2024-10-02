import unittest
from unittest.mock import patch, MagicMock
from consumer_config import initialize_consumer, commit_offsets, handle_errors
from confluent_kafka import KafkaException, KafkaError

class TestConsumerConfig(unittest.TestCase):

    @patch('consumer_config.Consumer')
    def test_initialize_consumer(self, mock_consumer):
        topic = 'test_topic'
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance

        consumer = initialize_consumer(topic)

        mock_consumer.assert_called_once_with({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'traffic_consumer_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        })
        mock_consumer_instance.subscribe.assert_called_once_with([topic])
        self.assertEqual(consumer, mock_consumer_instance)

    @patch('consumer_config.Consumer')
    def test_commit_offsets_success(self, mock_consumer):
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance
        mock_consumer_instance.commit.return_value = None  # Simulate successful commit

        consumer = mock_consumer_instance

        with patch('builtins.print') as mock_print:
            commit_offsets(consumer)

            mock_consumer_instance.commit.assert_called_once_with(asynchronous=False)
            mock_print.assert_called_once_with("Offsets committed successfully.")

    @patch('consumer_config.Consumer')
    def test_commit_offsets_failure(self, mock_consumer):
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance
        mock_consumer_instance.commit.side_effect = KafkaException('Commit failed')

        consumer = mock_consumer_instance

        with patch('builtins.print') as mock_print:
            commit_offsets(consumer)

            mock_consumer_instance.commit.assert_called_once_with(asynchronous=False)
            mock_print.assert_called_once_with('Error committing offsets: Commit failed')

    def test_handle_errors_partition_eof(self):
        msg = MagicMock()
        msg.error.return_value.code.return_value = KafkaError._PARTITION_EOF

        result = handle_errors(msg)

        self.assertTrue(result)

    def test_handle_errors_other(self):
        msg = MagicMock()
        msg.error.return_value.code.return_value = 1  # Some other error code
        msg.error.return_value = MagicMock()  # Mock the error object

        with patch('builtins.print') as mock_print:
            result = handle_errors(msg)

            self.assertFalse(result)
            mock_print.assert_called_once()

    def test_handle_errors_no_error(self):
        msg = MagicMock()
        msg.error.return_value = None  # No error

        result = handle_errors(msg)

        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
