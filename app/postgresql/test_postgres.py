import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from db_batch import (connect_db, check_db_connection, fetch_data_from_table,
                      fetch_population_make_table, fetch_population_cc_table,
                      fetch_incidents_over_time, fetch_unique_location,
                      fetch_report_incident, fetch_incident_count_today)
from db_stream import fetch_speedband_location, fetch_unique_location as fetch_unique_location_stream

# Mocking the database connection
@pytest.fixture(scope="function")
def mock_db_connection():
    with patch('db_batch.psycopg2.connect') as mock_connect:
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        yield mock_connection

def test_connect_db(mock_db_connection):
    """Test the database connection."""
    conn = connect_db()
    assert conn is not None
    mock_db_connection.assert_called_once()

def test_check_db_connection(mock_db_connection):
    """Test the check_db_connection function."""
    result = check_db_connection()
    assert result == "Successfully connected to PostgreSQL!"
    
    # Test when connection fails
    mock_db_connection.side_effect = Exception("Connection failed")
    result = check_db_connection()
    assert "Connection failed" in result

def test_fetch_data_from_table(mock_db_connection):
    """Test fetching data from a table."""
    mock_cursor = mock_db_connection.cursor.return_value
    mock_cursor.fetchall.return_value = [(1, 'data1'), (2, 'data2')]
    mock_cursor.description = [('id',), ('data',)]

    result = fetch_data_from_table('test_table')
    
    expected = [{'id': 1, 'data': 'data1'}, {'id': 2, 'data': 'data2'}]
    assert result == expected

def test_fetch_population_make_table(mock_db_connection):
    """Test fetching population by make."""
    mock_cursor = mock_db_connection.cursor.return_value
    mock_cursor.fetchall.return_value = [(2020, 100), (2021, 200)]

    result = fetch_population_make_table('cars', 'Toyota')
    
    expected = pd.DataFrame({'year': [2020, 2021], 'total_number': [100, 200]})
    pd.testing.assert_frame_equal(result, expected)

def test_fetch_population_cc_table(mock_db_connection):
    """Test fetching population by cc rating."""
    mock_cursor = mock_db_connection.cursor.return_value
    mock_cursor.fetchall.return_value = [(2020, 150), (2021, 250)]

    result = fetch_population_cc_table('cars', '1000cc')
    
    expected = pd.DataFrame({'year': [2020, 2021], 'total_number': [150, 250]})
    pd.testing.assert_frame_equal(result, expected)

def test_fetch_incidents_over_time(mock_db_connection):
    """Test fetching incidents over time."""
    mock_cursor = mock_db_connection.cursor.return_value
    mock_cursor.fetchall.return_value = [(pd.Timestamp('2024-10-01'), 5), (pd.Timestamp('2024-10-02'), 3)]

    result = fetch_incidents_over_time()

    expected = pd.DataFrame({"incident_date": [pd.Timestamp('2024-10-01'), pd.Timestamp('2024-10-02')], "incident_count": [5, 3]})
    pd.testing.assert_frame_equal(result, expected)

def test_fetch_unique_location(mock_db_connection):
    """Test fetching unique locations."""
    mock_cursor = mock_db_connection.cursor.return_value
    mock_cursor.fetchall.return_value = [('Road 1',), ('Road 2',)]

    result = fetch_unique_location()

    expected = [{'label': 'Road 1', 'value': 'Road 1'}, {'label': 'Road 2', 'value': 'Road 2'}]
    assert result == expected

def test_fetch_report_incident(mock_db_connection):
    """Test fetching report incident."""
    mock_cursor = mock_db_connection.cursor.return_value
    mock_cursor.fetchall.return_value = [(pd.Timestamp('2024-10-01'), 10), (pd.Timestamp('2024-10-02'), 5)]

    result = fetch_report_incident()

    expected = pd.DataFrame({"date": [pd.Timestamp('2024-10-01'), pd.Timestamp('2024-10-02')], "result": [10, 5]})
    pd.testing.assert_frame_equal(result, expected)

def test_fetch_incident_count_today(mock_db_connection):
    """Test fetching incident count for today."""
    mock_cursor = mock_db_connection.cursor.return_value
    mock_cursor.fetchone.return_value = (3,)

    result = fetch_incident_count_today()
    
    assert result == 3

def test_fetch_speedband_location(mock_db_connection):
    """Test fetching speedband location."""
    mock_cursor = mock_db_connection.cursor.return_value
    mock_cursor.fetchall.return_value = [(1, 'Road 1', 'Data 1'), (2, 'Road 2', 'Data 2')]
    mock_cursor.description = [('link_id',), ('road_name',), ('data',)]

    result = fetch_speedband_location('Road 1')

    expected = pd.DataFrame({'link_id': [1, 2], 'road_name': ['Road 1', 'Road 2'], 'data': ['Data 1', 'Data 2']})
    pd.testing.assert_frame_equal(result, expected)

if __name__ == "__main__":
    pytest.main()
