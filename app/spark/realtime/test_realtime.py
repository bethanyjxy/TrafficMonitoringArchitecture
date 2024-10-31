import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from stream_process import create_spark_session, read_kafka_stream, process_stream

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark = create_spark_session()
    yield spark
    spark.stop()

def test_create_spark_session():
    """Test the creation of Spark session."""
    spark_session = create_spark_session()
    assert spark_session is not None
    assert spark_session.version is not None

@patch("stream_process.SparkSession")
def test_read_kafka_stream(mock_spark_session, spark):
    """Test reading from Kafka stream."""
    mock_stream = MagicMock()
    mock_spark_session.readStream.return_value = mock_stream
    mock_stream.format.return_value.option.return_value.load.return_value = mock_stream

    kafka_broker = "kafka:9092"
    kafka_topics = "traffic_incidents,traffic_images,traffic_speedbands,traffic_vms"
    
    stream = read_kafka_stream(spark, kafka_broker, kafka_topics)

    assert stream is mock_stream
    mock_spark_session.readStream.assert_called_once()
    mock_stream.format.assert_called_once_with("kafka")
    mock_stream.option.assert_any_call("kafka.bootstrap.servers", kafka_broker)
    mock_stream.option.assert_any_call("subscribe", kafka_topics)

@patch("stream_process.udf")
@patch("stream_process.from_json")
@patch("stream_process.regexp_extract")
@patch("stream_process.regexp_replace")
@patch("stream_process.trim")
def test_process_stream(mock_trim, mock_regexp_replace, mock_regexp_extract, mock_from_json, mock_udf, spark):
    """Test processing the Kafka stream."""
    # Create a mock Kafka DataFrame
    mock_kafka_stream = MagicMock()
    mock_kafka_stream.filter.return_value = mock_kafka_stream
    mock_kafka_stream.withColumn.return_value = mock_kafka_stream
    mock_kafka_stream.select.return_value = mock_kafka_stream

    incident_stream, speedbands_stream, image_stream, vms_stream = process_stream(mock_kafka_stream)

    # Ensure the correct processing of incidents
    assert incident_stream is not None
    assert speedbands_stream is not None
    assert image_stream is not None
    assert vms_stream is not None
    mock_kafka_stream.filter.assert_any_call(mock_kafka_stream)

@patch("stream_process.SparkSession")
def test_write_to_postgres(mocker):
    """Test writing DataFrame to PostgreSQL."""
    df = MagicMock()
    table_name = "incident_table"
    postgres_url = {"url": "jdbc:postgresql://localhost/test"}
    postgres_properties = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

    mock_write = mocker.patch("stream_process.write_to_postgres", return_value=None)
    
    write_to_postgres(df, table_name, postgres_url, postgres_properties)

    mock_write.assert_called_once_with(df, table_name, postgres_url, postgres_properties)

if __name__ == "__main__":
    pytest.main()
