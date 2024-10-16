# test_stream.py
import pytest
from unittest.mock import patch, MagicMock
from postgres_stream import write_to_postgres, create_spark_session, read_kafka_stream, process_stream

# Mock data for testing
mock_df = MagicMock()  # Mock DataFrame
mock_postgres_url = {"url": "jdbc:postgresql://localhost/testdb"}
mock_postgres_properties = {"user": "test_user", "password": "test_pass"}

# Test case for writing DataFrame to PostgreSQL
def test_write_to_postgres():
    with patch('pyspark.sql.DataFrame.write.jdbc') as mock_write:
        write_to_postgres(mock_df, "test_table", mock_postgres_url, mock_postgres_properties)

        # Check if the jdbc write method was called with correct parameters
        mock_write.assert_called_once_with(
            url=mock_postgres_url['url'],
            table="test_table",
            mode="append",
            properties=mock_postgres_properties
        )

# Test case for create_spark_session
def test_create_spark_session():
    with patch('pyspark.sql.SparkSession.builder') as mock_builder:
        mock_spark = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark
        spark = create_spark_session()
        assert spark == mock_spark

# Test case for reading Kafka stream
def test_read_kafka_stream():
    with patch('pyspark.sql.SparkSession.readStream') as mock_read_stream:
        mock_kafka_stream = MagicMock()
        mock_read_stream.format.return_value.option.return_value.load.return_value = mock_kafka_stream
        
        spark = MagicMock()  # Mock Spark session
        kafka_stream = read_kafka_stream(spark, "localhost:9092", "topic1,topic2")

        mock_read_stream.format.assert_called_once_with("kafka")
        mock_read_stream.option.assert_any_call("kafka.bootstrap.servers", "localhost:9092")
        mock_read_stream.option.assert_any_call("subscribe", "topic1,topic2")
        mock_read_stream.option.assert_any_call("startingOffsets", "earliest")
        assert kafka_stream == mock_kafka_stream

# Test case for processing the stream
def test_process_stream():
    mock_kafka_stream = MagicMock()

    # Mock the return value of the processing function
    with patch('stream_process.from_json') as mock_from_json, \
         patch('stream_process.col') as mock_col, \
         patch('stream_process.regexp_extract') as mock_regexp_extract, \
         patch('stream_process.regexp_replace') as mock_regexp_replace, \
         patch('stream_process.trim') as mock_trim, \
         patch('stream_process.date_format') as mock_date_format, \
         patch('stream_process.udf') as mock_udf:

        mock_incident_stream = MagicMock()
        mock_speedbands_stream = MagicMock()
        mock_image_stream = MagicMock()
        mock_vms_stream = MagicMock()

        # Simulate the processing output
        mock_from_json.return_value = mock_incident_stream
        mock_col.return_value = mock_incident_stream
        mock_regexp_extract.return_value = mock_incident_stream
        mock_regexp_replace.return_value = mock_incident_stream
        mock_trim.return_value = mock_incident_stream
        mock_date_format.return_value = mock_incident_stream
        
        # Call the process_stream function
        incident_stream, speedbands_stream, image_stream, vms_stream = process_stream(mock_kafka_stream)

        # Ensure that streams were processed
        assert incident_stream == mock_incident_stream
        assert speedbands_stream == mock_speedbands_stream
        assert image_stream == mock_image_stream
        assert vms_stream == mock_vms_stream

# Run the tests
if __name__ == "__main__":
    pytest.main()
