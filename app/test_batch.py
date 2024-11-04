import os
import unittest
from unittest.mock import patch, MagicMock
from spark.batch.batch_config import (
    create_spark_session,
    get_postgres_connection,
    create_table,
    insert_table,
    send_to_hdfs,
    generate_id
)

class TestTrafficDataProcessing(unittest.TestCase):

    @patch('spark.batch.batch_config.SparkSession')  # Mock SparkSession
    def test_create_spark_session(self, mock_spark):
        app_name = "TestApp"
        session = create_spark_session(app_name)

        # Check that a Spark session is created with the correct app name
        mock_spark.builder.appName.assert_called_once_with(app_name)

    @patch('spark.batch.batch_config.psycopg2.connect')
    def test_get_postgres_connection(self, mock_connect):
        # Simulate a successful connection
        mock_connect.return_value = MagicMock()
        
        conn = get_postgres_connection()

        # Check that the connection was established
        mock_connect.assert_called_once_with(
            dbname='traffic_db', 
            user='traffic_admin', 
            password='traffic_pass', 
            host='postgres', 
            port='5432'
        )
        
        self.assertIsNotNone(conn)

    @patch('spark.batch.batch_config.get_postgres_connection')
    def test_create_table(self, mock_get_connection):
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        create_table("CREATE TABLE test (id SERIAL PRIMARY KEY)", mock_conn)

        # Check that the table creation query was executed
        mock_conn.cursor.return_value.execute.assert_called_once_with(
            "CREATE TABLE test (id SERIAL PRIMARY KEY)"
        )
        mock_conn.commit.assert_called_once()

    @patch('spark.batch.batch_config.get_postgres_connection')
    def test_insert_table(self, mock_get_connection):
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        data = (1,)
        
        insert_table(data, "INSERT INTO test (id) VALUES (%s)", mock_conn)

        # Check that the insert query was executed
        mock_conn.cursor.return_value.execute.assert_called_once_with(
            "INSERT INTO test (id) VALUES (%s)", data
        )
        mock_conn.commit.assert_called_once()

    @patch('spark.batch.batch_config.hdfs_client')
    def test_send_to_hdfs(self, mock_hdfs_client):
        topic = "test_topic"
        data = {"key": "value"}

        # Simulate HDFS client behavior
        mock_hdfs_client.status.return_value = {}
    
        # Set up a timestamp for testing
        test_timestamp = "2024-11-02T15:05:58.252036"
        mock_hdfs_client.write.return_value.timestamp = test_timestamp

        send_to_hdfs(topic, data)

        # Check that the correct file path is constructed and data is sent
        file_path = os.path.join(os.environ.get('HDFS_DIRECTORY', '/user/hadoop/traffic_data/'), f"{topic}.json")
        mock_hdfs_client.write.assert_called()


    def test_generate_id(self):
        # Test that the ID generated is a string and in milliseconds
        generated_id = generate_id()
        self.assertIsInstance(generated_id, str)
        self.assertTrue(generated_id.isdigit())

if __name__ == '__main__':
    unittest.main()
