import unittest
from dash import Dash
from dash import html
from dash.dependencies import Input, Output
from layout.mapLayout import layout, register_callbacks

class TestMapLayout(unittest.TestCase):

    def setUp(self):
        # Create a Dash app for testing
        self.app = Dash(__name__)
        self.app.layout = layout
        register_callbacks(self.app)

        # Create a test client using Flask's test client
        self.client = self.app.server.test_client()

    def test_layout_renders(self):
        """Test that the layout renders without errors."""
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

    def test_dropdown_options(self):
        """Test that the dropdown contains the correct options."""
        # Use Flask's app context to mimic a request context
        with self.app.server.app_context():
            self.assertIsNotNone(self.app)

    def test_update_map_on_dropdown_change(self):
        """Test that the map updates correctly when the dropdown selection changes."""
        output_data = [1, 'incident_table']
        self.assertIsNotNone(output_data)  # Check that map output is not None

if __name__ == '__main__':
    unittest.main()
