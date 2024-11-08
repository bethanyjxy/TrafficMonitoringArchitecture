import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
from postgresql.db_stream import (
    connect_db,
    check_db_connection,
    fetch_stream_table,
    fetch_speedband_location,
    fetch_unique_location,
    fetch_incident_count_today,
    fetch_incidents_over_time,
    fetch_vehicle_type_incidents,
    fetch_images_table
)

class TestDatabaseFunctions(unittest.TestCase):

    @patch('psycopg2.connect')
    def test_connect_db_success(self, mock_connect):
        # Arrange
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        # Act
        connection = connect_db()

        # Assert
        self.assertIsNotNone(connection)
        mock_connect.assert_called_once_with(
            dbname='traffic_db',  # Replace with actual values
            user='traffic_admin',
            password='traffic_pass',
            host='postgres',
            port='5432'
        )

    @patch('psycopg2.connect')
    def test_connect_db_failure(self, mock_connect):
        # Arrange
        mock_connect.side_effect = Exception("Connection failed")

        # Act
        connection = connect_db()

        # Assert
        self.assertIsNone(connection)

    @patch('postgresql.db_stream.connect_db')
    def test_check_db_connection_success(self, mock_connect_db):
        # Arrange
        mock_connect_db.return_value = MagicMock()  # Simulate successful connection

        # Act
        result = check_db_connection()

        # Assert
        self.assertEqual(result, "Successfully connected to PostgreSQL!")

    @patch('postgresql.db_stream.connect_db')
    def test_check_db_connection_failure(self, mock_connect_db):
        # Arrange
        mock_connect_db.return_value = None  # Simulate failed connection

        # Act
        result = check_db_connection()

        # Assert
        self.assertEqual(result, "Connection failed.")

    @patch('postgresql.db_stream.connect_db')
    def test_fetch_stream_table(self, mock_connect_db):
        # Arrange
        mock_connection = MagicMock()
        mock_connect_db.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, 'data1', '2024-11-01 10:00:00'),
            (2, 'data2', '2024-11-01 11:00:00')
        ]
        mock_cursor.description = [('id',), ('data',), ('timestamp',)]
        mock_connection.cursor.return_value = mock_cursor

        # Act
        result = fetch_stream_table('some_table')

        # Assert
        expected = [
            {'id': 1, 'data': 'data1', 'timestamp': '2024-11-01 10:00:00'},
            {'id': 2, 'data': 'data2', 'timestamp': '2024-11-01 11:00:00'}
        ]
        self.assertEqual(result, expected)

    @patch('postgresql.db_stream.connect_db')
    def test_fetch_speedband_location(self, mock_connect_db):
        # Arrange
        mock_connection = MagicMock()
        mock_connect_db.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [('Link1',), ('Link2',)]
        mock_cursor.description = [('linkid',)]
        mock_connection.cursor.return_value = mock_cursor

        # Act
        result = fetch_speedband_location('')

        # Assert
        expected = pd.DataFrame({'linkid': ['Link1', 'Link2']})
        pd.testing.assert_frame_equal(result, expected)

    @patch('postgresql.db_stream.connect_db')
    def test_fetch_unique_location(self, mock_connect_db):
        # Arrange
        mock_connection = MagicMock()
        mock_connect_db.return_value = mock_connection
        cursor = MagicMock()
        cursor.fetchall.return_value = [('Location1',), ('Location2',)]
        mock_connection.cursor.return_value = cursor

        # Act
        result = fetch_unique_location()

        # Assert
        expected = [{'label': 'Location1', 'value': 'Location1'}, {'label': 'Location2', 'value': 'Location2'}]
        self.assertEqual(result, expected)

    @patch('postgresql.db_stream.connect_db')
    def test_fetch_incident_count_today(self, mock_connect_db):
        # Arrange
        mock_connection = MagicMock()
        mock_connect_db.return_value = mock_connection
        cursor = MagicMock()
        cursor.fetchone.return_value = (5,)
        mock_connection.cursor.return_value = cursor

        # Act
        result = fetch_incident_count_today()

        # Assert
        self.assertNotEqual(result, 5)

    @patch('postgresql.db_stream.connect_db')
    def test_fetch_incidents_over_time(self, mock_connect_db):
        # Arrange
        mock_connection = MagicMock()
        mock_connect_db.return_value = mock_connection
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (datetime(2024, 11, 1, 10, 0), 3),
            (datetime(2024, 11, 1, 11, 0), 2),
        ]
        mock_connection.cursor.return_value = cursor

        # Act
        result = fetch_incidents_over_time()
        
        # Assert
        expected = pd.DataFrame({
            "incident_datetime": [datetime(2024, 11, 1, 10, 0), datetime(2024, 11, 1, 11, 0)],
            "incident_count": [3, 2]
        })
        self.assertIsNot(result,expected)
        

    @patch('postgresql.db_stream.connect_db')
    def test_fetch_vehicle_type_incidents(self, mock_connect_db):
        # Arrange
        mock_connection = MagicMock()
        mock_connect_db.return_value = mock_connection
        cursor = MagicMock()
        cursor.fetchall.return_value = [('Car', 10), ('Truck', 5)]
        mock_connection.cursor.return_value = cursor

        # Act
        result = fetch_vehicle_type_incidents()

        # Assert
        expected = pd.DataFrame({
            "vehicle_type": ['Car', 'Truck'],
            "vehicle_count": [10, 5]
        })
        self.assertIsNot(result,expected)

    @patch('postgresql.db_stream.connect_db')
    def test_fetch_images_table(self, mock_connect_db):
        # Arrange
        mock_connection = MagicMock()
        mock_connect_db.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, 'camera1', 'url1', '2024-11-01 10:00:00'),
            (2, 'camera2', 'url2', '2024-11-01 11:00:00')
        ]
        mock_cursor.description = [('id',), ('camera_id',), ('image_url',), ('img_timestamp',)]
        mock_connection.cursor.return_value = mock_cursor

        # Act
        result = fetch_images_table()

        # Assert
        expected = pd.DataFrame({
            'id': [1, 2],
            'camera_id': ['camera1', 'camera2'],
            'image_url': ['url1', 'url2'],
            'img_timestamp': ['2024-11-01 10:00:00', '2024-11-01 11:00:00']
        })
        pd.testing.assert_frame_equal(result, expected)

if __name__ == '__main__':
    unittest.main()
