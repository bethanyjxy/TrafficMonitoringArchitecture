from flask import Blueprint, render_template, request
import folium
from folium.plugins import HeatMap
import numpy as np
from postgresql.db_stream import *

def get_color_from_congestion(speedband):
    if speedband == 1:
        return 'green'        # Least congested
    elif speedband == 2:
        return 'yellowgreen'  # Slightly congested
    elif speedband == 3:
        return 'yellow'       # Moderately congested
    elif speedband == 4:
        return 'orange'       # Congested
    elif speedband == 5:
        return 'red'          # Most congested
    else:
        return 'black'        # Default color if speedband is invalid

# Define the blueprint for live traffic routes
live_traffic_blueprint = Blueprint('live_traffic_blueprint', __name__)

@live_traffic_blueprint.route('/live_traffic')
def live_traffic():

    incidents_data = fetch_stream_table('incident_table')
    """Display live traffic map with Folium based on selected filters."""
    filters = request.args.get('filters', '')
    selected_filters = filters.split(',') if filters else []

    # Map of filters to table names
    table_mapping = {
        'incidents': 'incident_table',
        'speedbands': 'speedbands_table',
        'images': 'image_table',
        'vms': 'vms_table'
    }

    # Colors for different filter types
    filter_colors = {
        'incidents': 'red',
        'speedbands': 'green',
        'images': 'purple',
        'vms': 'orange'
    }
    
    # Create a Folium map with smooth zooming behavior
    map_obj = folium.Map(
        location=[1.3521, 103.8198],
        zoom_start=20,
        max_zoom=24,
        prefer_canvas=True,  # Improves rendering performance when zooming
        tiles="cartodb positron"
    )

    heat_data = [] 

    # Add markers dynamically from the selected data types
    for filter_type in selected_filters:
        table_name = table_mapping.get(filter_type)
        if table_name:
            try:
                data = fetch_stream_table(table_name)
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
                        
                    
                    elif filter_type == 'speedbands':
                        start_lat = row.get('startlat')
                        start_lon = row.get('startlon')
                        end_lat = row.get('endlat')
                        end_lon = row.get('endlon')

                        if start_lat and start_lon and end_lat and end_lon:
                            # Generate intermediate points for the heatmap
                            num_points = 10  # Adjust number of points for desired granularity
                            latitudes = np.linspace(start_lat, end_lat, num_points)
                            longitudes = np.linspace(start_lon, end_lon, num_points)

                            # Collect heatmap points, using congestion level (1 to 8)
                            heat_value = row.get('speedband')  # Get the speedband as the congestion level
                            for lat, lon in zip(latitudes, longitudes):
                                # Append to heat_data list with intensity based on congestion level
                                heat_data.append([lat, lon, heat_value])  # Collect heatmap data

                            locations=[(start_lat, start_lon), (end_lat, end_lon)],
                            # Add a line representing the route with color based on congestion
                            folium.PolyLine(
                                color=get_color_from_congestion(heat_value),  # Color based on congestion level
                                weight=5,  # Thickness of the line
                                opacity=0.4  # Transparency of the line
                            ).add_to(map_obj)  # Add the line to the map  
                            # Use fit_bounds to adjust the map view to include the polyline
                            map_obj.fit_bounds(locations)                    
            except Exception as e:
                print(f"Error fetching data for {table_name}: {e}")

    # Render map as HTML
    map_html = map_obj._repr_html_()

    # Render the template with the map
    return render_template('liveTraffic.html', map_html=map_html, incidents=incidents_data)



