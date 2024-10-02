import unittest
from unittest.mock import patch, MagicMock
from postgres_stream import write_to_postgres, main

class PostgresStreamTestCase(unittest.TestCase):

    @patch('postgres_stream.SparkSession')
    @patch('postgres_stream.create_spark_session')
    @patch('postgres_stream.read_kafka_stream')
    @patch('postgres_stream.process_stream')
    @patch('postgres_stream.write_to_postgres')
    def test_main_success(self, mock_write_to_postgres, mock_process_stream, mock_read_kafka_stream, mock_create_spark_session, mock_spark_session):
        # Mock the Spark session
        mock_spark = MagicMock()
        mock_create_spark_session.return_value = mock_spark

        # Mock the Kafka stream and processing
        mock_kafka_stream = MagicMock()
        mock_read_kafka_stream.return_value = mock_kafka_stream
        
        incident_stream = MagicMock()
        speedbands_stream = MagicMock()
        image_stream = MagicMock()
        vms_stream = MagicMock()
        erp_stream = MagicMock()
        mock_process_stream.return_value = (incident_stream, speedbands_stream, image_stream, vms_stream, erp_stream)

        # Call the main function
        main()

        # Check if the necessary methods were called
        mock_create_spark_session.assert_called_once()
        mock_read_kafka_stream.assert_called_once_with(mock_spark, "localhost:9092", "traffic_incidents,traffic_images,traffic_speedbands,traffic_vms,traffic_erp")
        mock_process_stream.assert_called_once_with(mock_kafka_stream)

        # Check if write_to_postgres was set up for all streams
        incident_stream.writeStream.foreachBatch.assert_called_once()
        speedbands_stream.writeStream.foreachBatch.assert_called_once()
        image_stream.writeStream.foreachBatch.assert_called_once()
        vms_stream.writeStream.foreachBatch.assert_called_once()
        erp_stream.writeStream.foreachBatch.assert_called_once()

    @patch('postgres_stream.write_to_postgres')
    def test_write_to_postgres_success(self, mock_write_to_postgres):
        """Test the function for writing DataFrame to PostgreSQL."""
        mock_df = MagicMock()
        write_to_postgres(mock_df, 'test_table', 'test_url', {'user': 'user', 'password': 'pass'})
        mock_write_to_postgres.assert_called_once_with(mock_df, 'test_table', 'test_url', {'user': 'user', 'password': 'pass'})

    @patch('postgres_stream.write_to_postgres')
    def test_write_to_postgres_failure(self, mock_write_to_postgres):
        """Test error handling in writing to PostgreSQL."""
        mock_df = MagicMock()
        mock_write_to_postgres.side_effect = Exception("Database error")
        with self.assertRaises(Exception) as context:
            write_to_postgres(mock_df, 'test_table', 'test_url', {'user': 'user', 'password': 'pass'})
        self.assertEqual(str(context.exception), "Database error")

if __name__ == '__main__':
    unittest.main()
