import unittest
from unittest.mock import patch, MagicMock
from daily_incident import read_json_from_hdfs, main
from hist_process import (
    create_spark_session,
    read_cars_data,
    read_motorcycles_data,
    read_speed_data,
    read_traffic_lights_data
)

class TestDailyIncident(unittest.TestCase):

    @patch('daily_incident.spark')
    def test_read_json_from_hdfs(self, mock_spark):
        # Mock the JSON data
        mock_spark.read.json.return_value = MagicMock()
        result_df = read_json_from_hdfs(mock_spark, "test.json")
        self.assertIsNotNone(result_df)
        mock_spark.read.json.assert_called_once_with('hdfs://namenode:8020/user/hadoop/traffic_data/test.json')

    @patch('daily_incident.create_spark_session')
    @patch('daily_incident.get_postgres_connection')
    @patch('daily_incident.send_to_hdfs')
    @patch('daily_incident.read_json_from_hdfs')
    def test_main(self, mock_read_json, mock_send_to_hdfs, mock_get_postgres_connection, mock_create_spark_session):
        mock_spark = MagicMock()
        mock_create_spark_session.return_value = mock_spark
        mock_read_json.return_value = MagicMock()  # Mock DataFrame

        mock_df = mock_read_json.return_value
        mock_df.filter.return_value.count.return_value = 5  # Simulate 5 incidents today

        main()

        mock_create_spark_session.assert_called_once_with("DailyIncident_BatchReport")
        mock_read_json.assert_called_once_with(mock_spark, "traffic_incidents.json")
        mock_get_postgres_connection.assert_called_once()
        mock_send_to_hdfs.assert_called_once()

class TestHistoricalProcessing(unittest.TestCase):

    @patch('hist_process.SparkSession')
    def test_create_spark_session(self, mock_spark_session):
        app_name = "TestApp"
        spark = create_spark_session(app_name)
        self.assertIsNotNone(spark)
        mock_spark_session.builder.appName.assert_called_once_with(app_name)

    @patch('hist_process.read_csv_from_hdfs')
    @patch('hist_process.write_to_postgres')
    def test_read_cars_data(self, mock_write_to_postgres, mock_read_csv_from_hdfs):
        mock_spark = MagicMock()
        mock_make_df = MagicMock()
        mock_cc_df = MagicMock()
        mock_read_csv_from_hdfs.side_effect = [mock_make_df, mock_cc_df]

        read_cars_data(mock_spark)

        self.assertTrue(mock_read_csv_from_hdfs.called)
        self.assertEqual(mock_write_to_postgres.call_count, 2)

    @patch('hist_process.read_csv_from_hdfs')
    @patch('hist_process.write_to_postgres')
    def test_read_motorcycles_data(self, mock_write_to_postgres, mock_read_csv_from_hdfs):
        mock_spark = MagicMock()
        mock_make_df = MagicMock()
        mock_cc_df = MagicMock()
        mock_read_csv_from_hdfs.side_effect = [mock_make_df, mock_cc_df]

        read_motorcycles_data(mock_spark)

        self.assertTrue(mock_read_csv_from_hdfs.called)
        self.assertEqual(mock_write_to_postgres.call_count, 2)

    @patch('hist_process.read_csv_from_hdfs')
    @patch('hist_process.write_to_postgres')
    def test_read_speed_data(self, mock_write_to_postgres, mock_read_csv_from_hdfs):
        mock_spark = MagicMock()
        mock_speed_df = MagicMock()
        mock_read_csv_from_hdfs.return_value = mock_speed_df

        read_speed_data(mock_spark)

        mock_read_csv_from_hdfs.assert_called_once_with(mock_spark, "road_traffic_condition.csv", mock.ANY)
        mock_write_to_postgres.assert_called_once()

    @patch('hist_process.read_csv_from_hdfs')
    @patch('hist_process.write_to_postgres')
    def test_read_traffic_lights_data(self, mock_write_to_postgres, mock_read_csv_from_hdfs):
        mock_spark = MagicMock()
        mock_traffic_lights_df = MagicMock()
        mock_read_csv_from_hdfs.return_value = mock_traffic_lights_df

        read_traffic_lights_data(mock_spark)

        mock_read_csv_from_hdfs.assert_called_once_with(mock_spark, "annual_traffic_lights.csv", mock.ANY)
        mock_write_to_postgres.assert_called_once()

if __name__ == '__main__':
    unittest.main()
