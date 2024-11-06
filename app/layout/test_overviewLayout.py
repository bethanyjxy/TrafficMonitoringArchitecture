import unittest
from unittest.mock import patch
from dash import Dash
from overviewLayout import layout, register_callbacks

class TestOverviewLayout(unittest.TestCase):

    @patch('overviewLayout.fetch_incident_count_today')
    def test_update_incident_count(self, mock_fetch):
        # Mock the return value of fetch_incident_count_today
        mock_fetch.return_value = 5
        
        # Create a Dash app for testing
        app = Dash(__name__)
        app.layout = layout
        register_callbacks(app)

        # Simulate the callback execution
        with app.test_client() as client:
            response = client.get('/')
            self.assertEqual(mock_fetch.call_count, 1)

            # Trigger the callback
            with app.test_client() as client:
                response = client.get('/')

            # Check that the incident count is displayed correctly
            self.assertIn(b'5', response.data)

    @patch('overviewLayout.fetch_incidents_over_time')
    def test_update_trend_chart(self, mock_fetch):
        # Mock the return value for incidents over time
        mock_fetch.return_value = [{'incident_date': '2023-09-01', 'incident_count': 10},
                                    {'incident_date': '2023-09-02', 'incident_count': 20}]
        
        app = Dash(__name__)
        app.layout = layout
        register_callbacks(app)

        # Simulate the callback execution
        with app.test_client() as client:
            response = client.get('/')
            self.assertEqual(mock_fetch.call_count, 1)

            # Trigger the callback
            response = client.get('/')

            # Verify the trend chart data
            self.assertIn(b'Incident Trends Over Time', response.data)

    @patch('overviewLayout.fetch_vms_incident_correlation')
    def test_update_correlation_chart(self, mock_fetch):
        # Mock the return value for VMS and incident correlation
        mock_fetch.return_value = [{'incident_count': 10, 'vms_message': 'Message 1'},
                                    {'incident_count': 20, 'vms_message': 'Message 2'}]

        app = Dash(__name__)
        app.layout = layout
        register_callbacks(app)

        # Simulate the callback execution
        with app.test_client() as client:
            response = client.get('/')
            self.assertEqual(mock_fetch.call_count, 1)

            # Trigger the callback
            response = client.get('/')

            # Check if the correlation chart data is being processed
            self.assertIn(b'Correlation Between VMS Messages and Incidents', response.data)

    @patch('overviewLayout.fetch_vehicle_type_incidents')
    def test_update_pie_chart(self, mock_fetch):
        # Mock the return value for vehicle type incidents
        mock_fetch.return_value = [{'vehicle_type': 'Car', 'vehicle_count': 10},
                                    {'vehicle_type': 'Truck', 'vehicle_count': 15}]
        
        app = Dash(__name__)
        app.layout = layout
        register_callbacks(app)

        # Simulate the callback execution
        with app.test_client() as client:
            response = client.get('/')
            self.assertEqual(mock_fetch.call_count, 1)

            # Trigger the callback
            response = client.get('/')

            # Check if the pie chart is rendered
            self.assertIn(b'Incidents by Vehicle Type', response.data)

if __name__ == '__main__':
    unittest.main()
