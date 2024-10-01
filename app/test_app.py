import unittest
from flask import Flask
from dash import Dash
from app import server, traffic_app, overview_app

class AppTestCase(unittest.TestCase):

    def setUp(self):
        """Set up the Flask test client before each test."""
        self.app = server
        self.client = self.app.test_client()
        self.app.config['TESTING'] = True

    def test_flask_app_initialization(self):
        """Test if the Flask app is initialized correctly."""
        self.assertIsInstance(self.app, Flask)

    def test_traffic_app_initialization(self):
        """Test if the traffic Dash app is initialized correctly."""
        self.assertIsInstance(traffic_app, Dash)

    def test_overview_app_initialization(self):
        """Test if the overview Dash app is initialized correctly."""
        self.assertIsInstance(overview_app, Dash)

    def test_render_map_route(self):
        """Test the /map/ route."""
        response = self.client.get('/map/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'<!DOCTYPE html>', response.data)  # Check if the response contains HTML

    def test_traffic_overview_route(self):
        """Test the /overview/ route."""
        response = self.client.get('/overview/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'<!DOCTYPE html>', response.data)  # Check if the response contains HTML

if __name__ == '__main__':
    unittest.main()
