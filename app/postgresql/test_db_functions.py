import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from db_functions import (
    connect_db,
    check_db_connection,
    fetch_data_from_table,
    fetch_incident_count_today,
    fetch_incidents_over_time,
    fetch_vehicle_type_incidents,
    fetch_vms_incident_correlation,
    fetch_recent_images
)

class TestDBFunctions(unittest.TestCase):

    @patch('db_functions.psycopg2.connect')
    def test_connect_db_success(self, mock_connect):
        # Mock the connection object
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        connection = connect_db()
        self.assertIsNotNone(connection)
        mock_connect.assert_called_once()

    @patch('db_functions.psycopg2.connect')
    def test_connect_db_failure(self, mock_connect):
        mock_connect.side_effect = Exception("Connection failed")
        
        connection = connect_db()
        self.assertIsNone(connection)

    @patch('db_functions.connect_db')
    def test_check_db_connection_success(self, mock_connect):
        mock_connect.return_value = MagicMock()  # Simulate a successful connection
        result = check_db_connection()
        self.assertEqual(result, "Successfully connected to PostgreSQL!")

    @patch('db_functions.connect_db')
    def test_check_db_connection_failure(self, mock_connect):
        mock_connect.return_value = None  # Simulate a failed connection
        result = check_db_connection()
        self.assertEqual(result, "Connection failed.")

    @patch('db_functions.connect_db')
    @patch('db_functions.psycopg2.connect')
    def test_fetch_data_from_table(self, mock_connect, mock_db):
        mock_connect.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = [
            (1, 'data1'), (2, 'data2')
        ]
        mock_connect.return_value.cursor.return_value.__enter__.return_value.description = [('id',), ('data',)]
        
        data = fetch_data_from_table('test_table')
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['id'], 1)
        self.assertEqual(data[0]['data'], 'data1')

    @patch('db_functions.connect_db')
    def test_fetch_incident_count_today(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (5,)
        
        count = fetch_incident_count_today()
        self.assertEqual(count, 5)

    @patch('db_functions.connect_db')
    def test_fetch_incidents_over_time(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value.fetchall.return_value = [
            ('2023-09-01 12:00:00', 3),
            ('2023-09-02 12:00:00', 5),
        ]
        mock_connection.cursor.return_value.__enter__.return_value.description = [('incident_datetime',), ('incident_count',)]
        
        df = fetch_incidents_over_time()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.columns.tolist(), ['incident_date', 'incident_count'])

    @patch('db_functions.connect_db')
    def test_fetch_vehicle_type_incidents(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value.fetchall.return_value = [
            ('Car', 10),
            ('Truck', 5),
        ]
        mock_connection.cursor.return_value.__enter__.return_value.description = [('vehicle_type',), ('vehicle_count',)]
        
        df = fetch_vehicle_type_incidents()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    @patch('db_functions.connect_db')
    def test_fetch_vms_incident_correlation(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value.fetchall.return_value = [
            ('Message 1', 2),
            ('Message 2', 3),
        ]
        mock_connection.cursor.return_value.__enter__.return_value.description = [('vms_message',), ('incident_count',)]
        
        df = fetch_vms_incident_correlation()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    @patch('db_functions.connect_db')
    def test_fetch_recent_images(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value.fetchall.return_value = [
            (1, 'image1.jpg'),
            (2, 'image2.jpg'),
        ]
        mock_connection.cursor.return_value.__enter__.return_value.description = [('id',), ('image_url',)]
        
        images = fetch_recent_images()
        self.assertEqual(len(images), 2)

if __name__ == '__main__':
    unittest.main()
