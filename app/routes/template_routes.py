from flask import Blueprint, render_template, send_from_directory, request
from dash import Dash
import folium
from folium.plugins import MarkerCluster
from postgresql.db_functions import *

# Create a Blueprint for template-related routes
templates_blueprint = Blueprint('templates_blueprint', __name__)

# Route to serve static files
@templates_blueprint.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

# Route to landing page
@templates_blueprint.route('/')
def serve_main():
    return render_template('main.html')

# Route for Dashboard, served by Dash
@templates_blueprint.route('/dashboard')
def redirect_to_dashboard():
    return Dash.__getattr__('index')(Dash)  # Ensure `app` from Dash is correctly called


# Define the blueprint for live traffic routes
live_traffic_blueprint = Blueprint('live_traffic_blueprint', __name__)

@live_traffic_blueprint.route('/live_traffic')
def live_traffic():
    erp_data = fetch_data_from_table('erp_table')

    incidents_data = fetch_data_from_table('incident_table')
    """Display live traffic map with Folium based on selected filters."""
    filters = request.args.get('filters', '')
    selected_filters = filters.split(',') if filters else []

    # Map of filters to table names
    table_mapping = {
        'incidents': 'incident_table',
        'erp': 'erp_table',
        'speedbands': 'speedbands_table',
        'images': 'image_table',
        'vms': 'vms_table'
    }

    # Colors for different filter types
    filter_colors = {
        'incidents': 'red',
        'erp': 'blue',
        'speedbands': 'green',
        'images': 'purple',
        'vms': 'orange'
    }

    # Create a Folium map centered on Singapore
    map_obj = folium.Map(location=[1.3521, 103.8198], zoom_start=12)

    # Add markers dynamically from the selected data types
    for filter_type in selected_filters:
        table_name = table_mapping.get(filter_type)
        if table_name:
            try:
                data = fetch_data_from_table(table_name)
                if not data:
                    print(f"No data found for {table_name}")
                for row in data:
                    color = filter_colors.get(filter_type, 'black')
                    lat = row.get('latitude')
                    lon = row.get('longitude')

                    # Check for specific types and handle data accordingly
                    if filter_type == 'incidents' and lat and lon:
                        folium.Marker(
                            [lat, lon],
                            popup=row.get('message'), 
                            icon=folium.Icon(color=color)
                        ).add_to(map_obj)
                    elif filter_type == 'speedbands':
                        start_lat = row.get('startlat')
                        start_lon = row.get('startlon')
                        if start_lat and start_lon:
                            folium.Marker(
                                [start_lat, start_lon],
                                popup=row.get('roadname'), 
                                icon=folium.Icon(color=color)
                            ).add_to(map_obj)
                    elif filter_type == 'images' and lat and lon:
                        image_link = row.get('imagelink')
                        folium.Marker(
                            [lat, lon],
                            popup=folium.Popup(f'<img src="{image_link}" width="150" height="150">', max_width=250),
                            icon=folium.Icon(color=color)
                        ).add_to(map_obj)
                    elif filter_type == 'vms' and lat and lon:
                        folium.Marker(
                            [lat, lon],
                            popup=row.get('message'),
                            icon=folium.Icon(color=color)
                        ).add_to(map_obj)
            except Exception as e:
                print(f"Error fetching data for {table_name}: {e}")

    # Render map as HTML
    map_html = map_obj._repr_html_()

    # Render the template with the map
    return render_template('liveTraffic.html', map_html=map_html, erp_data=erp_data, incidents=incidents_data)


@templates_blueprint.route('/traffic_overview')
def traffic_overview():

    return render_template('trafficOverview.html')
