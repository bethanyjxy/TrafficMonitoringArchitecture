import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from spark.realtime.stream_process import create_spark_session, read_kafka_stream, process_stream

class TestStreamProcessing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestKafkaToSparkStream") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()

    def test_create_spark_session(self):
        # Act
        spark = create_spark_session()
        
        # Assert
        self.assertIsNotNone(spark)

    @patch('spark.realtime.stream_process.SparkSession')
    def test_read_kafka_stream(self, mock_spark_session):
        # Arrange
        mock_kafka_stream = MagicMock()
        mock_spark_session.readStream.format.return_value.option.return_value.option.return_value.load.return_value = mock_kafka_stream
        
        kafka_broker = "kafka:9092"
        kafka_topics = "traffic_incidents,traffic_images,traffic_speedbands,traffic_vms"

        # Act
        result_stream = read_kafka_stream(mock_spark_session, kafka_broker, kafka_topics)

        # Assert
        mock_spark_session.readStream.format.assert_called_with("kafka")
        mock_spark_session.readStream.format.return_value.option.assert_called_with("kafka.bootstrap.servers", kafka_broker)
        mock_spark_session.readStream.format.return_value.option.return_value.option.assert_called_with("subscribe", kafka_topics)
        self.assertNotEqual(result_stream, mock_kafka_stream)

    def test_process_stream(self):
        # Arrange
        kafka_stream = self.spark.createDataFrame(
            [("traffic_incidents", '{"Type": "Accident", "Latitude": 34.05, "Longitude": -118.25, "Message": "(10/02)12:30 Accident reported"}')],
            ["topic", "value"]
        )

        # Act
        incident_stream, speedbands_stream, image_stream, vms_stream = process_stream(kafka_stream)

        # Assert: Check if the incident_stream has the expected columns
        incident_columns = {field.name for field in incident_stream.schema.fields}
        expected_columns = {"Type", "Latitude", "Longitude", "Message", "incident_date", "incident_time", "incident_message", "timestamp"}
        self.assertTrue(expected_columns.issubset(incident_columns))

        # Similar assertions can be added for speedbands_stream, image_stream, and vms_stream as needed
        self.assertIsNotNone(speedbands_stream)
        self.assertIsNotNone(image_stream)
        self.assertIsNotNone(vms_stream)

if __name__ == '__main__':
    unittest.main()
