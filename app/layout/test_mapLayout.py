import unittest
from dash import Dash
from dash import html
from dash.dependencies import Input, Output
from mapLayout import layout, register_callbacks

class TestMapLayout(unittest.TestCase):

    def setUp(self):
        # Create a Dash app for testing
        self.app = Dash(__name__)
        self.app.layout = layout
        register_callbacks(self.app)

        # Create a test client
        self.client = self.app.test_client()

    def test_layout_renders(self):
        """Test that the layout renders without errors."""
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

    def test_dropdown_options(self):
        """Test that the dropdown contains the correct options."""
        with self.app.test_request_context():
            dropdown = self.app.layout.children[1].children[0].children
            options = [option['value'] for option in dropdown.options]
            self.assertEqual(options, ['incident_table', 'vms_table', 'image_table'])

    def test_update_map_on_dropdown_change(self):
        """Test that the map updates correctly when the dropdown selection changes."""
        # Mocking the data fetching and setting it up can be done here
        with self.app.test_request_context():
            # Simulate a change in the dropdown
            self.app.callback_map['update_map']['inputs'][0].update(n_intervals=1)
            self.app.callback_map['update_map']['inputs'][1].update(value='incident_table')
            
            # Trigger the callback manually (you might need to adjust this based on your actual logic)
            output = self.app.callback_map['update_map']['callback'](1, 'incident_table')
            self.assertIsNotNone(output[0])  # Check that map output is not None
            self.assertIsNotNone(output[1])  # Check that table output is not None

if __name__ == '__main__':
    unittest.main()
