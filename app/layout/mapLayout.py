# mapLayout.py
import dash_bootstrap_components as dbc
from postgresql.db_stream import *
import plotly.express as px
import pandas as pd
import numpy as np 
from dash import html,dcc,dash_table
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import requests

# Function to fetch image data from the API and return a DataFrame
def fetch_image():
    url = "https://api.data.gov.sg/v1/transport/traffic-images"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # Check if 'items' exists and is not empty
        if 'items' in data and len(data['items']) > 0:
            camera_data = []
            
            # Iterate over the 'items' to extract camera information
            for item in data['items']:
                for camera in item.get('cameras', []):
                    camera_id = camera.get('camera_id')
                    image_url = camera.get('image')
                    latitude = camera['location']['latitude']
                    longitude = camera['location']['longitude']
                    
                    # Append camera details to the list
                    camera_data.append({
                        'camera_id': camera_id,
                        'image_url': image_url,
                        'latitude': latitude,
                        'longitude': longitude
                    })
            
            # Convert to DataFrame
            camera_df = pd.DataFrame(camera_data)
            return camera_df
    return None

def fetch_image_data(camera_id):
    url = "https://api.data.gov.sg/v1/transport/traffic-images"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

        if 'items' in data and len(data['items']) > 0:
            # Iterate through the "items" to find the "cameras" list
            for item in data['items']:
                # Iterate through each camera in the cameras list
                for camera in item.get('cameras', []):
                    # If the camera_id matches, return the image URL
                    if camera.get('camera_id') == str(camera_id):
                        return camera['image']
    
    return None

# Define the layout
layout = html.Div([
    # Page Title
    html.H3('Real-Time Live Traffic', className="text-left mb-4"),
    
    # Dropdown to select the table
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(
                id='table-selector',
                options=[  
                    {'label': 'Traffic Incident', 'value': 'incident_table'},
                    {'label': 'Traffic Flow', 'value': 'speedbands_table'},
                    {'label': 'Traffic Authorities Message', 'value': 'vms_table'},
                    {'label': 'Camera Location', 'value': 'image_table'}
                ],
                value='incident_table'  # Default table
            ),
            width=3, 
            className="mb-4"  
        )
    ], justify="left"), 
    
    # Row for the location dropdown
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(
                id='location-dropdown',  # This dropdown appears only when speedbands_table is selected
                placeholder="Select a location",
                options=[],  # Populated dynamically
                value=None,
            ),
            width=3
        )
    ], id='location-row', style={'display': 'none'}),  # Hide initially
    
    # Row for the map
    dbc.Row([
        dbc.Col(
            html.Div(id='map-output', style={'padding': '10px'}),
            width=12,  
            className="shadow-sm p-3 mb-4 bg-white rounded" ,
            id = "general-map-row",
        )
    ], justify="center"),  
    
    # Row for the map and image container
    dbc.Row([  
        dbc.Col(
            html.Div([
                dcc.Graph(id='camera-map'),
            ]),
            width=7,  
            className="shadow-sm p-3 mb-4 bg-white rounded",
            id = "camera-map-row",
            style={'display':'none'},
        ),
        dbc.Col(
            html.Div(id='image-container', style={'marginTop': '20px', 'textAlign': 'center'}),
            width=5, 
            className="shadow-sm p-3 mb-4 bg-white rounded",
            id = "image-container-row",
            style={'display':'none'}
            
        ),
    ], justify="center") , 
    

    # Row for the incident table below the map
    dbc.Row([
        dbc.Col([
            html.H3('Recent Incidents', className="text-left mb-4"),
            html.Div(id='incident-table')
        ], width=12, className="shadow-sm p-3 mb-4 bg-white rounded")  
    ], justify="center"), 
    
    
    # Auto-refresh every min
    dcc.Interval(id='interval-component', interval=60*1000, n_intervals=0)
], style={'max-width': '100%', 'margin': '0 auto', 'padding': '20px', 'overflow-x': 'hidden'})




# Define callback registration 
def register_callbacks(app):
    
    #Function to toggle location drop down when speedbands_table is selected 
    @app.callback(
        Output('location-row', 'style'),
        Input('table-selector', 'value')
    )
    def toggle_location_dropdown(selected_table):
        if selected_table == 'speedbands_table':
            return {'display': 'block'}  # Show the dropdown
        else:
            return {'display': 'none'}  # Hide the dropdown
        
    # Update Location dropdown selection    
    @app.callback(
    Output('location-dropdown', 'options'),
    Output('location-dropdown', 'value'), 
    Input('location-row', 'id')  
        )
    def update_location_options(_):
        option = fetch_unique_location()
        default_value = option[0] if option else None
        return option, default_value

    # Update Notification Table
    @app.callback(
        Output('incident-table', 'children'),
        Input('interval-component', 'n_intervals')
    )
    def update_table(n):
        data = fetch_stream_table('incident_table')
        df = pd.DataFrame(data)
        
        if df.empty:
                return html.P("No data available."), None
        df = df.sort_values(by=['incident_date', 'incident_time'], ascending=[False, False])
        
        # Create incident table to display recent incidents
        incident_table_component = dash_table.DataTable(
        id='incident-table',
        columns=[
                {"name": "Date", "id": "incident_date"},
                {"name": "Time", "id": "incident_time"},
                {"name": "Incident", "id": "incident_message"}
            ],
        data=[],  # Initially set to empty to force re-render
        style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
        style_cell={'textAlign': 'left', 'fontSize': 12, 'font-family': 'Arial', 'padding': '5px'},
        page_size=10
        )
        incident_table_component.data = df[["incident_date", "incident_time", "incident_message"]].to_dict('records')

        return incident_table_component
    
    # Function to change from general map to Camera Map 
    @app.callback(
        [
            Output('general-map-row', 'style'),     # Hide the general map
            Output('camera-map-row', 'style'),      # Show the camera map
            Output('image-container-row', 'style')  # Show the image container
        ],
        Input('table-selector', 'value')            # Get the selected table value
    )
    def toggle_maps(selected_table):
        if selected_table == 'image_table':
            #Show the camera map & image container
            return {'display': 'none'}, {'display': 'block'}, {'display': 'block'}
        else:
            #Show the general map , hide the camera map & image container
            return {'display': 'block'}, {'display': 'none'}, {'display': 'none'}
        
    # Function to display Camera Image Map
    @app.callback(
        [Output('camera-map', 'figure'),        # Update the figure of the graph
        Output('image-container', 'children')],  # Update the image container
        [Input('table-selector', 'value'),      # Input to fetch data based on selected table
        Input('camera-map', 'clickData')]       # Listen for click events on the map
    )
    def update_camera_map(selected_table, click_data):
        # Fetch image table
        #image_df = fetch_images_table()
        # Fetch image
        camera_df = fetch_image()  
        
        #df = pd.merge(image_df, camera_df[['camera_id', 'image_url']], left_on='cameraid', right_on='camera_id', how='left')

        # Create the figure
        fig = go.Figure(go.Scattermapbox(
            lat=camera_df['latitude'],
            lon=camera_df['longitude'],
            mode='markers',
            marker=dict(size=12,sizemode='diameter'),
            hoverinfo='text',
            text=camera_df['camera_id'],  # Show camera ID on hover
        ))

        # Set the map style
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(center=dict(lat=1.3521, lon=103.8198), zoom=11),
            height=600,  
            width=1200, 
            margin={"r":0,"t":0,"l":0,"b":0},
        )

        # Default image content when no camera is clicked
        image_content = html.Div([
            html.H2(f"Select a camera on the map", style={"color": "darkblue"}),
        ])

        # Check if there is click data
        if click_data is not None:
            # Extract the camera ID based on the clickData
            point_number = click_data['points'][0]['pointNumber']
            selected_camera = camera_df.iloc[point_number]
            
            camera_id = selected_camera['camera_id']
            image_src = selected_camera['image_url']
            
            # Create image content to show in the container
            image_content = html.Div([
                html.Img(src=image_src, style={"width": "100%"}),
                html.H2(f"Camera ID: {camera_id}", style={"color": "darkblue"}),
            ], style={ 'white-space': 'normal', 'margin-right': '30px'})

        return fig, image_content  # Return both the updated figure and the image container 
        
    # Function to update General Map   
    @app.callback(
        Output('map-output', 'children'),
        [Input('interval-component', 'n_intervals'), Input('table-selector', 'value'), Input('location-dropdown', 'value')]
    )
    def update_map(n, selected_table, location):

        data = fetch_stream_table(selected_table)
        df = pd.DataFrame(data)

        if selected_table == 'incident_table':
            # Ensure data is available
            if df.empty:
                return html.P("No data available."), None

            fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="type", hover_data=["message"],
                                    color="type", zoom=11, height=600, width=1385)

            # Set map style and marker behavior on hover
            fig.update_traces(marker=dict(size=14, sizemode='area'), 
                          selector=dict(mode='markers'),
                          hoverinfo='text',
                          hoverlabel=dict(bgcolor="white", font_size=12, font_family="Arial"))
        
            fig.update_layout(
                mapbox_style="open-street-map",
                mapbox=dict(
                center=dict(lat=1.3521, lon=103.8198),  # Singapore coordinates
                zoom=11
            ),               
                margin={"r": 0, "t": 0, "l": 0, "b": 0} # Remove margins
            )

            fig.update_traces(marker=dict(sizemode="diameter", size=12, opacity=0.7))
            
            return dcc.Graph(figure=fig)
        
        elif selected_table == 'vms_table':
            # Ensure data is available
            if df.empty:
                return html.P("No data available."), None
            
            # Scatter map with VMS locations as points
            fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="message",
                                    zoom=11, height=600,width=1200)

            # Set map style and marker behavior on hover
            fig.update_traces(marker=dict(size=10, sizemode='area'),  
                            selector=dict(mode='markers'),
                            hoverinfo='text',
                            hoverlabel=dict(bgcolor="white", font_size=12))

            # Use Mapbox open street map style
            fig.update_layout(
                mapbox_style="open-street-map",
                mapbox=dict(
                    center=dict(lat=1.3521, lon=103.8198),  # Singapore coordinates
                    zoom=11
                ),
                margin={"r":0,"t":0,"l":0,"b":0},  # Remove margins
                
            )
            fig.update_traces(marker=dict(sizemode="diameter", size=12, opacity=0.7))

            return dcc.Graph(figure=fig), None
        
        elif selected_table == 'speedbands_table':
            if location is None:
                # Fetch Road name if no location is selected
                speed_df = fetch_speedband_location("") 

                # Create a Plotly figure for displaying road names
                fig = px.scatter_mapbox(
                    speed_df, 
                    lat="startlat", 
                    lon="startlon", 
                    hover_name="roadname",  
                    zoom=11, 
                    height=600, 
                    width=1200
                )

                # Set map style and marker behavior on hover
                fig.update_traces(
                    marker=dict(size=10, sizemode='area'), 
                    selector=dict(mode='markers'),
                    hoverinfo='text',
                    hoverlabel=dict(bgcolor="white", font_size=12),
                    hovertext=speed_df['roadname'],
                )

                # Use Mapbox open street map style
                fig.update_layout(
                    mapbox_style="open-street-map",
                    mapbox=dict(
                        center=dict(lat=1.3521, lon=103.8198),  # Singapore coordinates
                        zoom=11
                    ),
                    margin={"r": 0, "t": 0, "l": 0, "b": 0},  # Remove margins
                )

                fig.update_traces(marker=dict(sizemode="diameter", size=12, opacity=0.7))

                return dcc.Graph(figure=fig), None

            else:
                # If a specific location is selected, fetch the location-based polylines
                speed_df = fetch_speedband_location(location)  
                polylines = []  # To collect polyline data

                # Loop over each row in the fetched data
                for i, row in speed_df.iterrows():
                    start_lat = row['startlat']
                    start_lon = row['startlon']
                    end_lat = row['endlat']
                    end_lon = row['endlon']
                    cog_lvl = row['speedband']  # Get the congestion level

                    # Check for valid coordinates
                    if pd.notna(start_lat) and pd.notna(start_lon) and pd.notna(end_lat) and pd.notna(end_lon):
                        # Determine polyline color and traffic label based on congestion level (speedband)
                        if cog_lvl <= 2:
                            line_color = 'red'
                            traffic_label = "Heavy Traffic"
                        elif 3 <= cog_lvl <= 5:
                            line_color = 'orange'
                            traffic_label = "Moderate Traffic"
                        else:
                            line_color = 'green'
                            traffic_label = "Light Traffic"

                        # Create polyline segments
                        polylines.append({
                            "lat": [start_lat, end_lat],
                            "lon": [start_lon, end_lon],
                            "color": line_color,
                            "traffic_label": traffic_label  # Label for the traffic condition
                        })

                if not polylines:
                    # If no polylines were created, handle accordingly
                    return html.P("No Speedband Data Available for this Location")

                # Create a Plotly figure for displaying polylines
                fig = go.Figure()

                # Add polylines to the map
                for polyline in polylines:
                    fig.add_trace(go.Scattermapbox(
                        lat=polyline["lat"],
                        lon=polyline["lon"],
                        mode='lines',
                        line=dict(width=8, color=polyline['color']),  # Line color based on congestion level
                        name=polyline['traffic_label'],  # Dynamic label based on traffic condition
                        hoverinfo='text',  # Show custom hover text
                        hovertext=polyline['traffic_label'],  # Full display of traffic label
                        showlegend=False
                    ))

                # Set map zoom level and center
                zoom_level = 16 if location else 13  # Higher zoom for specific location, lower for overall map

                if polylines:
                    lat_center = np.mean([p["lat"][0] for p in polylines])
                    lon_center = np.mean([p["lon"][0] for p in polylines])
                else:
                    lat_center, lon_center = 0, 0  # Fallback center if no polylines exist

                fig.update_layout(
                    mapbox_style="open-street-map",
                    mapbox=dict(
                        center=dict(lat=lat_center, lon=lon_center),
                        zoom=zoom_level,  # Set the zoom level based on location selection
                    ),
                    height=600,
                    width=1200,
                    margin={"r": 0, "t": 0, "l": 0, "b": 0},  # Remove margins
                )

                return dcc.Graph(figure=fig) 