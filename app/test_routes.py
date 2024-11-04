import unittest
from unittest.mock import patch, MagicMock
from flask import Flask
from routes.liveTraffic_routes import live_traffic_blueprint

class TestRoutes(unittest.TestCase):

    def setUp(self):
        """Create a test client for the Flask app."""
        self.app = Flask(__name__)
        self.app.register_blueprint(live_traffic_blueprint)
        self.client = self.app.test_client()

    @patch('routes.liveTraffic_routes.fetch_stream_table')
    def test_live_traffic(self, mock_fetch_stream_table):
        """Test the /live_traffic route."""
        # Mock data for incidents and other types
        mock_incidents_data = [{'latitude': 1.3521, 'longitude': 103.8198, 'message': 'Incident here'}]
        mock_speedbands_data = [{'startlat': 1.3521, 'startlon': 103.8198, 'endlat': 1.3550, 'endlon': 103.8200, 'speedband': 3}]
        mock_fetch_stream_table.side_effect = [mock_incidents_data, mock_speedbands_data]

        # Simulate a request to the live traffic route with filters
        response = self.client.get('/live_traffic?filters=incidents,speedbands')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Incident here', response.data)  # Check if the incident message is in the response

    @patch('routes.liveTraffic_routes.fetch_stream_table')
    def test_live_traffic_no_data(self, mock_fetch_stream_table):
        """Test the /live_traffic route when no data is returned."""
        mock_fetch_stream_table.return_value = []  # Simulate no data found
        response = self.client.get('/live_traffic?filters=incidents,speedbands')
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b'Incident here', response.data)  # No incident message should be found

    @patch('routes.liveTraffic_routes.fetch_stream_table')
    def test_live_traffic_with_invalid_filter(self, mock_fetch_stream_table):
        """Test the /live_traffic route with an invalid filter."""
        mock_incidents_data = [{'latitude': 1.3521, 'longitude': 103.8198, 'message': 'Incident here'}]
        mock_fetch_stream_table.return_value = mock_incidents_data  # Only incidents data
        response = self.client.get('/live_traffic?filters=invalid_filter')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Incident here', response.data)  # Check that incident is still in the response

    @patch('routes.liveTraffic_routes.fetch_stream_table')
    def test_live_traffic_with_multiple_filters(self, mock_fetch_stream_table):
        """Test the /live_traffic route with multiple valid filters."""
        mock_incidents_data = [{'latitude': 1.3521, 'longitude': 103.8198, 'message': 'Incident here'}]
        mock_speedbands_data = [{'startlat': 1.3521, 'startlon': 103.8198, 'endlat': 1.3550, 'endlon': 103.8200, 'speedband': 2}]
        mock_fetch_stream_table.side_effect = [mock_incidents_data, mock_speedbands_data]

        response = self.client.get('/live_traffic?filters=incidents,speedbands')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Incident here', response.data)  # Incident message should be present

    @patch('routes.liveTraffic_routes.fetch_stream_table')
    def test_live_traffic_error_handling(self, mock_fetch_stream_table):
        """Test the /live_traffic route with an error in fetching data."""
        mock_fetch_stream_table.side_effect = Exception("Database error")
        
        response = self.client.get('/live_traffic?filters=incidents')
        self.assertEqual(response.status_code, 500) 
        
if __name__ == '__main__':
    unittest.main()
