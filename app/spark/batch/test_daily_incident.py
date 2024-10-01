import unittest
from unittest.mock import patch, MagicMock
from daily_incident import write_to_postgres, generate_id, main

class DailyIncidentTestCase(unittest.TestCase):

    @patch('daily_incident.SparkSession')
    @patch('daily_incident.write_to_postgres')
    @patch('daily_incident.datetime')
    @patch('daily_incident.time')
    def test_main_success(self, mock_time, mock_datetime, mock_write_to_postgres, mock_spark_session):
        # Mocking time and datetime to control their outputs
        mock_time.time.return_value = 1234567890  # Mock the time function
        mock_datetime.now.return_value.strftime.return_value = "090124"  # Mock date format

        # Create a mock DataFrame
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df  # Return itself for chaining
        mock_df.count.return_value = 5  # Mocking the count result
        mock_df.withColumn.return_value = mock_df  # Chaining

        # Set up the SparkSession mock to return a mock DataFrame
        mock_spark = mock_spark_session.return_value
        mock_spark.read.format.return_value.load.return_value = mock_df

        # Call the main function
        main()

        # Check if the DataFrame methods were called correctly
        mock_df.withColumn.assert_called()  # Check if withColumn was called
        mock_df.filter.assert_called()  # Check if filter was called
        self.assertEqual(mock_df.count.call_count, 1)  # Ensure count is called once

        # Check if write_to_postgres was called with expected parameters
        mock_write_to_postgres.assert_called_once()

    @patch('daily_incident.time')
    def test_generate_id(self, mock_time):
        """Test the ID generation function."""
        mock_time.time.return_value = 1234567890  # Mock the time function
        unique_id = generate_id()
        self.assertEqual(unique_id, '1234567890000')  # Check if the ID is generated correctly

    @patch('daily_incident.write_to_postgres')
    def test_write_to_postgres_success(self, mock_write_to_postgres):
        """Test the function for writing DataFrame to PostgreSQL."""
        # Mock the DataFrame
        mock_df = MagicMock()
        write_to_postgres(mock_df, 'test_table', 'test_url', {'user': 'user', 'password': 'pass'})
        mock_write_to_postgres.assert_called_once_with(mock_df, 'test_table', 'test_url', {'user': 'user', 'password': 'pass'})

    @patch('daily_incident.write_to_postgres')
    def test_write_to_postgres_failure(self, mock_write_to_postgres):
        """Test error handling in writing to PostgreSQL."""
        mock_df = MagicMock()
        mock_write_to_postgres.side_effect = Exception("Database error")
        with self.assertRaises(Exception) as context:
            write_to_postgres(mock_df, 'test_table', 'test_url', {'user': 'user', 'password': 'pass'})
        self.assertEqual(str(context.exception), "Database error")

if __name__ == '__main__':
    unittest.main()
