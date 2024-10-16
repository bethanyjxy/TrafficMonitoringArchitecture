import os
import json
import pytest
from unittest.mock import patch, MagicMock
from batch_config import send_to_hdfs, get_postgres_connection, create_table, insert_table, generate_id
from daily_incident import main, read_json_from_hdfs
from pyspark.sql import SparkSession

# Mock data for testing
mock_data = {
    "ID": "12345",
    "Name": "Test Report",
    "Result": 10,
    "Date": "2024-10-10"
}

# Test case for sending data to HDFS
def test_send_to_hdfs():
    with patch('hdfs.InsecureClient') as mock_client:
        mock_hdfs_client = MagicMock()
        mock_client.return_value = mock_hdfs_client
        
        send_to_hdfs("test_topic", mock_data)

        # Check if HDFS write was called
        mock_hdfs_client.write.assert_called()
        assert mock_hdfs_client.write.call_count == 2  # One for creating and one for appending to the file

# Test case for PostgreSQL connection
def test_get_postgres_connection():
    with patch('psycopg2.connect') as mock_connect:
        mock_connect.return_value = MagicMock()
        
        conn = get_postgres_connection()
        
        # Verify the connection was established
        assert conn is not None
        mock_connect.assert_called_once()

# Test case for creating a table
def test_create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS report_incident (
        ID VARCHAR(255) PRIMARY KEY
    );
    """
    
    mock_conn = MagicMock()
    with patch('psycopg2.connect', return_value=mock_conn):
        create_table(create_table_query, mock_conn)
        
        # Verify that the execute was called
        mock_conn.cursor().execute.assert_called_once_with(create_table_query)

# Test case for inserting data into PostgreSQL
def test_insert_table():
    insert_query = """
        INSERT INTO report_incident (ID, Name, Result, Date)
        VALUES (%s, %s, %s, %s)
    """
    
    mock_conn = MagicMock()
    with patch('psycopg2.connect', return_value=mock_conn):
        insert_table(mock_data.values(), insert_query, mock_conn)
        
        # Verify that the execute was called
        mock_conn.cursor().execute.assert_called_once_with(insert_query, tuple(mock_data.values()))

# Test case for reading JSON from HDFS
def test_read_json_from_hdfs():
    mock_spark = MagicMock(spec=SparkSession)
    mock_spark.read.json.return_value = MagicMock()
    
    with patch('daily_incident.create_spark_session', return_value=mock_spark):
        df = read_json_from_hdfs(mock_spark, "test_file.json")
        
        # Verify that the JSON read function was called
        mock_spark.read.json.assert_called_once_with("hdfs://namenode:8020/user/hadoop/traffic_data/test_file.json")
        assert df is not None

# Test case for main function in daily_incident.py
def test_main_function():
    with patch('daily_incident.read_json_from_hdfs') as mock_read:
        mock_spark = MagicMock(spec=SparkSession)
        mock_read.return_value = MagicMock()
        mock_read.return_value.withColumn.return_value.filter.return_value.count.return_value = 10

        with patch('daily_incident.get_postgres_connection', return_value=MagicMock()) as mock_conn:
            with patch('daily_incident.send_to_hdfs') as mock_send:
                main()  # Call the main function

                # Verify that the data is inserted
                mock_conn().cursor().execute.assert_any_call(
                    "INSERT INTO report_incident (ID, Name, Result, Date) VALUES (%s, %s, %s, %s)",
                    (mock.ANY, mock.ANY, 10, mock.ANY)
                )
                mock_send.assert_called_once()

# Run the tests
if __name__ == "__main__":
    pytest.main()
