import unittest
from unittest.mock import patch, MagicMock
from flask import Flask
from liveTraffic_routes import live_traffic_blueprint, templates_blueprint

class TestRoutes(unittest.TestCase):

    def setUp(self):
        """Create a test client for the Flask app."""
        self.app = Flask(__name__)
        self.app.register_blueprint(live_traffic_blueprint)
        self.app.register_blueprint(templates_blueprint)
        self.client = self.app.test_client()

    @patch('liveTraffic_routes.fetch_data_from_table')
    def test_live_traffic_route(self, mock_fetch_data):
        """Test the /live_traffic route."""
        mock_fetch_data.return_value = [{'latitude': 1.3521, 'longitude': 103.8198, 'message': 'Incident here'}]
        
        response = self.client.get('/live_traffic')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Incident here', response.data)  # Check if incident message is in the response

    def test_serve_static_files(self):
        """Test the /static/<path:filename> route."""
        response = self.client.get('/static/testfile.txt')  # Assume testfile.txt exists in your static folder
        self.assertEqual(response.status_code, 200)

    def test_serve_main_page(self):
        """Test the landing page route."""
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Main Page Content', response.data)  # Replace with actual content check

    def test_redirect_to_dashboard(self):
        """Test the dashboard redirect."""
        with patch('dash.Dash.__getattr__') as mock_redirect:
            mock_redirect.return_value = MagicMock()
            response = self.client.get('/dashboard')
            self.assertEqual(response.status_code, 200)  # Adjust based on your actual redirect behavior

    def test_traffic_overview(self):
        """Test the traffic overview route."""
        response = self.client.get('/traffic_overview')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Traffic Overview Content', response.data)  # Replace with actual content check

    def test_traffic_insight(self):
        """Test the traffic insight route."""
        response = self.client.get('/traffic_insight')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Traffic Insight Content', response.data)  # Replace with actual content check

if __name__ == '__main__':
    unittest.main()
