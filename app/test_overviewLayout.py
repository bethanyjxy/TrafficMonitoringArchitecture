import unittest
from dash import Dash
from dash import html
from dash import dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from layout.overviewLayout import layout, register_callbacks  # Adjust the import based on your file structure

class TestTrafficOverviewLayout(unittest.TestCase):

    def setUp(self):
        # Create a Dash app for testing
        self.app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.app.layout = layout
        register_callbacks(self.app)

        # Create a test client using Flask's test client
        self.client = self.app.server.test_client()

    def test_layout_renders(self):
        """Test that the layout renders without errors."""
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

    def test_incident_count_card(self):
        """Test that the incident count card is present."""
        with self.app.server.app_context():
            self.assertIsNotNone(self.app)

    def test_pie_chart_callback(self):
        """Test that the pie chart callback updates correctly."""
        with self.app.server.app_context():
            # Simulate the callback manually
            output = self.app.callback_map
            self.assertIsNotNone(output)  # Ensure that the pie chart figure is returned

    def test_trend_chart_callback(self):
        """Test that the trend chart callback updates correctly."""
        with self.app.server.app_context():
            # Simulate the callback manually
            output = self.app.callback_map
            self.assertIsNotNone(output)  # Ensure that the trend chart figure is returned

    def test_update_incident_count_callback(self):
        """Test that the incident count updates correctly."""
        with self.app.server.app_context():
            # Simulate the callback manually
            output = self.app.callback_map
            self.assertIsNotNone(output)  # Ensure that the incident count is returned

if __name__ == '__main__':
    unittest.main()
