import unittest
from flask import Flask
from liveTraffic_routes import live_traffic_blueprint
from postgresql.db_functions import fetch_data_from_table

class LiveTrafficRoutesTestCase(unittest.TestCase):
    def setUp(self):
        """Set up a Flask test client and register the blueprint."""
        self.app = Flask(__name__)
        self.app.register_blueprint(live_traffic_blueprint)
        self.client = self.app.test_client()
    
    def test_live_traffic_route(self):
        """Test the /live_traffic route."""
        with self.app.test_request_context('/live_traffic?filters=incidents,erp'):
            # Mock the data fetching function
            fetch_data_from_table = lambda table_name: [
                {'latitude': 1.3521, 'longitude': 103.8198, 'message': 'Incident 1'},
                {'latitude': 1.3522, 'longitude': 103.8199, 'message': 'Incident 2'},
            ] if table_name == 'incident_table' else []

            # Call the route
            response = self.client.get('/live_traffic?filters=incidents,erp')

            # Check the response status code
            self.assertEqual(response.status_code, 200)

            # Check if the map HTML is in the response
            self.assertIn(b'var map = L.map', response.data)  # Check for Leaflet map initialization

            # Check if the markers are correctly added
            self.assertIn(b'Incident 1', response.data)
            self.assertIn(b'Incident 2', response.data)

    def test_live_traffic_route_no_data(self):
        """Test the /live_traffic route when no data is returned."""
        with self.app.test_request_context('/live_traffic?filters=incidents'):
            fetch_data_from_table = lambda table_name: []

            response = self.client.get('/live_traffic?filters=incidents')

            self.assertEqual(response.status_code, 200)
            self.assertIn(b'No data found for incident_table', response.data)  # Expect a print statement in response

if __name__ == '__main__':
    unittest.main()
