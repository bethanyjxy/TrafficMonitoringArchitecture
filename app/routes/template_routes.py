from flask import Blueprint, render_template, send_from_directory
from dash import Dash
import folium
from postgresql.db_functions import fetch_data_from_table

# Create a Blueprint for template-related routes
templates_blueprint = Blueprint('templates_blueprint', __name__)

# Route to serve static files
@templates_blueprint.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

# Route to landing page
@templates_blueprint.route('/')
def serve_main():
    return send_from_directory('templates', 'main.html')

# Route for Dashboard, served by Dash
@templates_blueprint.route('/dashboard')
def redirect_to_dashboard():
    return Dash.__getattr__('index')(Dash)  # Ensure `app` from Dash is correctly called



# Define the blueprint for live traffic routes
live_traffic_blueprint = Blueprint('live_traffic_blueprint', __name__)

@live_traffic_blueprint.route('/live_traffic')
def live_traffic():
    """Display live traffic map with Folium."""
    try:
        # Fetch data from the incident_table
        incidents = fetch_data_from_table('incident_table')
    except Exception as e:
        # Log the error and provide user feedback
        print(f"Error fetching Incident data: {e}")
        incidents = []  # In case of error, show an empty map
    
    # Create a Folium map centered on Singapore
    map_obj = folium.Map(location=[1.3521, 103.8198], zoom_start=12)

    # Add markers dynamically from incidents data
    for row in incidents:
        folium.Marker(
            [row['latitude'], row['longitude']], 
            popup=row['message']
        ).add_to(map_obj)

    # Render map as HTML
    map_html = map_obj._repr_html_()
    return render_template('liveTraffic.html', map_html=map_html)
