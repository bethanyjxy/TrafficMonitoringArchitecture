import unittest
from flask import Flask
from template_routes import templates_blueprint

class TemplateRoutesTestCase(unittest.TestCase):
    def setUp(self):
        """Set up a Flask test client and register the blueprint."""
        self.app = Flask(__name__)
        self.app.register_blueprint(templates_blueprint)
        self.client = self.app.test_client()
    
    def test_serve_static(self):
        """Test serving a static file."""
        # You can create a temporary static file for testing if needed
        response = self.client.get('/static/test_file.txt')  # Replace with an actual test file
        self.assertEqual(response.status_code, 200)  # Check if the file is served successfully

    def test_serve_main(self):
        """Test the landing page route."""
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)  # Check if the status code is OK
        self.assertIn(b'<title>Main Page</title>', response.data)  # Replace with a specific title or element from main.html

    def test_redirect_to_dashboard(self):
        """Test the dashboard redirect route."""
        response = self.client.get('/dashboard')
        self.assertEqual(response.status_code, 200)  # Ensure it returns a successful status code
        self.assertIn(b'Dashboard', response.data)  # Replace with an expected element in the dashboard

    def test_traffic_overview(self):
        """Test the traffic overview route."""
        response = self.client.get('/traffic_overview')
        self.assertEqual(response.status_code, 200)  # Ensure it returns a successful status code
        self.assertIn(b'Traffic Overview', response.data)  # Replace with an expected element in trafficOverview.html

if __name__ == '__main__':
    unittest.main()
