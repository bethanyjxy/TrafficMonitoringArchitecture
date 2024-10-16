import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from postgresql.db_functions import (
    connect_db,
    check_db_connection,
    fetch_data_from_table,
    fetch_population_make_table,
    fetch_population_cc_table,
    fetch_population_year_table,
    fetch_unique_type_table,
    fetch_incident_count_today,
    fetch_incidents_over_time,
    fetch_vehicle_type_incidents,
    fetch_vms_incident_correlation,
    fetch_speedband_location,
    fetch_unique_location,
    fetch_recent_images
)

class TestPostgresFunctions(unittest.TestCase):

    @patch('postgresql.db_functions.psycopg2.connect')
    def test_connect_db_success(self, mock_connect):
        mock_connect.return_value = MagicMock()
        connection = connect_db()
        self.assertIsNotNone(connection)

    @patch('postgresql.db_functions.psycopg2.connect')
    def test_connect_db_failure(self, mock_connect):
        mock_connect.side_effect = Exception("Connection error")
        connection = connect_db()
        self.assertIsNone(connection)

    @patch('postgresql.db_functions.connect_db')
    def test_check_db_connection_success(self, mock_connect_db):
        mock_connect_db.return_value = MagicMock()
        result = check_db_connection()
        self.assertEqual(result, "Successfully connected to PostgreSQL!")

    @patch('postgresql.db_functions.connect_db')
    def test_check_db_connection_failure(self, mock_connect_db):
        mock_connect_db.return_value = None
        result = check_db_connection()
        self.assertEqual(result, "Connection failed.")

    @patch('postgresql.db_functions.connect_db')
    def test_fetch_data_from_table(self, mock_connect_db):
        mock_conn = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, 'data1'), (2, 'data2')]
        mock_cursor.description = [('id',), ('data',)]

        result = fetch_data_from_table('test_table')
        expected = [{'id': 1, 'data': 'data1'}, {'id': 2, 'data': 'data2'}]
        self.assertEqual(result, expected)

    @patch('postgresql.db_functions.connect_db')
    def test_fetch_population_make_table(self, mock_connect_db):
        mock_conn = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(2021, 100), (2022, 150)]
        
        df = fetch_population_make_table('cars', 'Toyota')
        expected_df = pd.DataFrame({'year': [2021, 2022], 'total_number': [100, 150]})
        pd.testing.assert_frame_equal(df, expected_df)

    @patch('postgresql.db_functions.connect_db')
    def test_fetch_incident_count_today(self, mock_connect_db):
        mock_conn = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [5]

        count = fetch_incident_count_today()
        self.assertEqual(count, 5)

    @patch('postgresql.db_functions.connect_db')
    def test_fetch_incidents_over_time(self, mock_connect_db):
        mock_conn = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('2024-01-01', 2), ('2024-01-02', 3)]
        
        df = fetch_incidents_over_time()
        expected_df = pd.DataFrame({'incident_date': ['2024-01-01', '2024-01-02'], 'incident_count': [2, 3]})
        pd.testing.assert_frame_equal(df, expected_df)

    @patch('postgresql.db_functions.connect_db')
    def test_fetch_vehicle_type_incidents(self, mock_connect_db):
        mock_conn = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('Car', 10), ('Motorcycle', 5)]
        
        df = fetch_vehicle_type_incidents()
        expected_df = pd.DataFrame({'vehicle_type': ['Car', 'Motorcycle'], 'vehicle_count': [10, 5]})
        pd.testing.assert_frame_equal(df, expected_df)

    @patch('postgresql.db_functions.connect_db')
    def test_fetch_unique_location(self, mock_connect_db):
        mock_conn = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('Location1',), ('Location2',)]
        
        result = fetch_unique_location()
        expected = [{'label': 'Location1', 'value': 'Location1'}, {'label': 'Location2', 'value': 'Location2'}]
        self.assertEqual(result, expected)

    # Add more test cases for other functions as needed...

if __name__ == '__main__':
    unittest.main()
