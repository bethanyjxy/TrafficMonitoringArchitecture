import unittest
from dash import Dash, html
import dash_bootstrap_components as dbc
from insightLayout import layout, register_callbacks  # Adjust the import based on your file structure

class TestInsightLayout(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        cls.app.layout = layout
        register_callbacks(cls.app)
        cls.client = cls.app.test_client()

    def test_layout_structure(self):
        # Check that the main components are present in the layout
        with self.client:
            response = self.client.get('/')
            self.assertIn(b'Traffic Insights', response.data)
            self.assertIn(b'Correlation Between VMS Messages and Incidents', response.data)
            self.assertIn(b'Incident Trends Over Time', response.data)
            self.assertIn(b'Annual Traffic Light Installations', response.data)
            self.assertIn(b'Annual Road Traffic Conditions', response.data)
            self.assertIn(b'Vehicle Population', response.data)

    def test_correlation_chart_update(self):
        # Test if the correlation chart updates correctly
        with self.client:
            response = self.client.get('/')
            initial_data = response.data

            # Trigger an update on the correlation chart
            self.app.callback_map  # Simulate interval update

            updated_response = self.client.get('/')
            self.assertNotEqual(initial_data, updated_response.data)  # Ensure the data is updated

    def test_population_graph_buttons(self):
        # Test button click behavior for car and motorcycle selection
        with self.client:
            response = self.client.get('/')
            initial_data = response.data
            
            # Simulate clicking the motorcycle button
            self.app.callback_map['update_button_colors']['callback'](0, 1)  # Car: 0 clicks, Motorcycle: 1 click
            
            updated_response = self.client.get('/')
            self.assertIn(b'color-secondary', updated_response.data)  # Ensure motorcycle button is highlighted
            self.assertIn(b'color-primary', updated_response.data)  # Ensure car button is not highlighted

    def test_filter_dropdown_options(self):
        # Test if filter dropdown updates options correctly
        with self.client:
            response = self.client.get('/')
            options = self.app.callback_map['update_type_dropdown']['callback']('cc', 1, 0)  # Filter type 'cc', Car button clicked

            # After updating, check if options are correctly populated
            self.assertIsInstance(options, list)
            self.assertGreater(len(options), 0)  # Ensure there are options

if __name__ == '__main__':
    unittest.main()
