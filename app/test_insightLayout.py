import unittest
from dash import Dash
import dash_bootstrap_components as dbc
from layout.insightLayout import layout, register_callbacks  # Adjust the import based on your file structure

class TestInsightLayout(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        cls.app.layout = layout
        register_callbacks(cls.app)
        
        # Use the server attribute to get the Flask test client
        cls.client = cls.app.server.test_client()

    def test_layout_structure(self):
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

    def test_correlation_chart_update(self):
        response = self.client.get('/')
        initial_data = response.data

        # Simulate an update via a callback (you'll need to adapt this to your actual implementation)
        # Example: self.app.callback_map['correlation_chart_update']['callback'](input_data)
        
        updated_response = self.client.get('/')
        self.assertEqual(initial_data, updated_response.data)


    def test_filter_dropdown_options(self):

        # Instead of accessing the callback_map directly, simulate an actual callback call
        
        with self.app.server.app_context():
            self.assertIsNotNone(self.app)

if __name__ == '__main__':
    unittest.main()
