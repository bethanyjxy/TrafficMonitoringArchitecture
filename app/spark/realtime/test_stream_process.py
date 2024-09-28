import unittest
from unittest.mock import patch, MagicMock
from stream_process import create_spark_session, read_kafka_stream, process_stream, write_to_console

class StreamProcessTestCase(unittest.TestCase):

    @patch('stream_process.SparkSession')
    def test_create_spark_session(self, mock_spark_session):
        """Test if the Spark session is created successfully."""
        spark = create_spark_session()
        self.assertIsNotNone(spark)
        mock_spark_session.builder.appName.assert_called_once_with("KafkaToSparkStream")

    @patch('stream_process.SparkSession')
    def test_read_kafka_stream(self, mock_spark_session):
        """Test reading Kafka streams."""
        mock_spark = MagicMock()
        mock_spark_session.return_value = mock_spark
        mock_kafka_stream = MagicMock()
        mock_spark.readStream.format.return_value.option.return_value.load.return_value = mock_kafka_stream

        kafka_broker = "localhost:9092"
        kafka_topics = "traffic_incidents,traffic_images,traffic_speedbands,traffic_vms,traffic_erp"
        
        result_stream = read_kafka_stream(mock_spark, kafka_broker, kafka_topics)

        self.assertEqual(result_stream, mock_kafka_stream)
        mock_spark.readStream.format.assert_called_once_with("kafka")
        mock_spark.readStream.format.return_value.option.assert_any_call("kafka.bootstrap.servers", kafka_broker)
        mock_spark.readStream.format.return_value.option.assert_any_call("subscribe", kafka_topics)

    @patch('stream_process.from_json')
    @patch('stream_process.regexp_extract')
    @patch('stream_process.regexp_replace')
    @patch('stream_process.trim')
    @patch('stream_process.date_format')
    def test_process_stream(self, mock_date_format, mock_trim, mock_regexp_replace, mock_regexp_extract, mock_from_json):
        """Test processing of Kafka stream."""
        mock_kafka_stream = MagicMock()

        # Set up mock return values
        mock_from_json.return_value = MagicMock()
        mock_regexp_extract.return_value = MagicMock()
        mock_regexp_replace.return_value = MagicMock()
        mock_trim.return_value = MagicMock()
        mock_date_format.return_value = MagicMock()

        incident_stream, speedbands_stream, image_stream, vms_stream, erp_stream = process_stream(mock_kafka_stream)

        # Validate the incident stream processing
        self.assertIsNotNone(incident_stream)
        mock_kafka_stream.filter.assert_called_once_with(col("topic") == "traffic_incidents")
        mock_regexp_extract.assert_called()
        mock_trim.assert_called()
        mock_date_format.assert_called()

    @patch('stream_process.print')
    def test_write_to_console(self, mock_print):
        """Test writing to console."""
        mock_df = MagicMock()
        write_to_console(mock_df, "test_table")
        mock_df.show.assert_called_once_with(truncate=False)

if __name__ == '__main__':
    unittest.main()
