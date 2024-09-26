# mapLayout.py
import dash_bootstrap_components as dbc
from postgresql.db_functions import *
import plotly.express as px
import pandas as pd
import numpy as np 
from dash import html,dcc,dash_table
from dash.dependencies import Input, Output
import plotly.graph_objects as go

# Define the layout
layout = html.Div([
    # Page Title
    html.H3('Real-Time Live Traffic', className="text-center mb-4"),
    
    # Dropdown to select the table
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(
                id='table-selector',
                style={"width": "70%"},
                options=[
                    {'label': 'Traffic Flow', 'value': 'speedbands_table'},
                    {'label': 'Traffic Incident', 'value': 'incident_table'},
                    {'label': 'Traffic Authorities Message', 'value': 'vms_table'},
                    {'label': 'Camera Location', 'value': 'image_table'}
                ],
                value='incident_table'  # Default table
            ),
            width=6,  # Dropdown takes half the width
            className="mb-4"  # Add some bottom margin for spacing
        )
    ], justify="center"),  # Center the dropdown
    
    # Div to display map and table
    dbc.Row([
        # Column for the map
        dbc.Col(
            html.Div(id='map-output', style={'padding': '10px'}),
            width=8,  # Map takes up more space
            className="shadow-sm p-3 mb-4 bg-white rounded"  # Add some styling and spacing
        ),
        # Column for the incident table
        dbc.Col([
            html.H4('Recent Incidents', className="text-center mb-4"),
            html.Div(id='incident-table')
        ], width=4, className="shadow-sm p-3 mb-4 bg-white rounded")  # Table takes up less space and has similar styling
    ], justify="space-between", style={'padding': '0 15px'}),  # Ensure consistent padding
    
    # Auto-refresh every 2 seconds
    dcc.Interval(id='interval-component', interval=2*1000, n_intervals=0)
], style={'max-width': '100%', 'margin': '0 auto', 'padding': '20px', 'overflow-x': 'hidden'})

# Define callback registration as a separate function
def register_callbacks(app):
    @app.callback(
        [Output('map-output', 'children'), Output('incident-table', 'children')],
        [Input('interval-component', 'n_intervals'), Input('table-selector', 'value')]
    )
    def update_map(n, selected_table):
        data = fetch_data_from_table(selected_table)
        df = pd.DataFrame(data)

        if selected_table == 'incident_table':
            if df.empty:
                return html.P("No data available."), None

            fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="type", hover_data=["message"],
                                    color="type", zoom=11, height=600, width=1200)

            # Set map style and marker behavior on hover
            fig.update_traces(marker=dict(size=14, sizemode='area'),  # Default marker size
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

            df = df.sort_values(by=['incident_date', 'incident_time'], ascending=[False, False])

            fig.update_traces(marker=dict(sizemode="diameter", size=12, opacity=0.7))
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

            return dcc.Graph(figure=fig), incident_table_component


        elif selected_table == 'image_table':
            #traffic_images = fetch_recent_images()
            #img_df = pd.DataFrame(traffic_images)
            fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="location", zoom=11, height=400,width=1000)
            fig.update_traces(marker=dict(size=12, sizemode='area'),  # Default marker size
                    selector=dict(mode='markers'),
                    hoverinfo='text',
                    hoverlabel=dict(bgcolor="white", font_size=16))

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

        elif selected_table == 'vms_table':
            # Ensure the data is available
            if df.empty:
                return html.P("No data available.")
            
            # Scatter map with VMS locations as points
            fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="message",
                                    zoom=11, height=600,width=1200)

            # Set map style and marker behavior on hover
            fig.update_traces(marker=dict(size=10, sizemode='area'),  # Default marker size
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
            if df.empty:
                    return html.P("No data available.")

            # Create a heatmap trace
            heat_data = []  # List to collect heatmap data

            # Loop over each row in the dataframe to create route points
            for i, row in df.iterrows():
                start_lat = row['startlat']
                start_lon = row['startlon']
                end_lat = row['endlat']
                end_lon = row['endlon']
                
                if start_lat and start_lon and end_lat and end_lon:
                    # Generate intermediate points for the heatmap
                    num_points = 10  # Adjust number of points for desired granularity
                    latitudes = np.linspace(start_lat, end_lat, num_points)
                    longitudes = np.linspace(start_lon, end_lon, num_points)
                    heat_value = row['speedband']  # Get the congestion level (1 to 5)

                    # Collect heatmap points
            for lat, lon in zip(latitudes, longitudes):
                # Check for congestion levels
                if heat_value == 0:  # No congestion
                    heat_data.append((lat, lon, 1))  # Assign a low intensity for no congestion (green)
                elif heat_value >= 1 and heat_value <= 8:
                    heat_data.append((lat, lon, heat_value))  # Append the congestion level directly

            # Create a heatmap trace if there is data
            if heat_data:
                lat, lon, intensity = zip(*heat_data)  # Unzip heat_data into separate lists

               # Define a custom colorscale for levels 1 to 5
                custom_colorscale = [
                    [0, 'darkred'],        # Level 1: 0 < 9 (no congestion)
                    [0.125, 'red'], # Level 2: 10 < 19
                    [0.25, 'orange'],     # Level 3: 20 < 29
                    [0.375, 'orange'],    # Level 4: 30 < 39
                    [0.5, 'yellow'],  # Level 5: 40 < 49
                    [0.625, 'yellow'],       # Level 6: 50 < 59
                    [0.75, 'green'],    # Level 7: 60 < 69
                    [0.875, 'green'],  # Level 8: 70 or more
                    [1, 'green']         # Optional: no congestion
                ]

                fig = go.Figure(data=go.Densitymapbox(
                    lat=lat,
                    lon=lon,
                    z=intensity,  # Use congestion level as intensity
                    colorscale=custom_colorscale,  # Use custom colorscale
                    radius=10,  # Adjust for visualization
                    zmin=1,  # Minimum congestion level
                    zmax=8  # Maximum congestion level
                ))

                # Update the layout for the map visualization
                fig.update_layout(
                    mapbox_style="open-street-map",  # OpenStreetMap style
                    mapbox=dict(center=dict(lat=np.mean(lat), lon=np.mean(lon)), zoom=11),  # Center the map
                    height=600,
                    width=1200,
                )
                

                # Add annotations for congestion levels
                traffic_conditions = {
                    2: ("Heavy", 'red'),
                    5: ("Moderate", 'orange'),
                    8: ("Light", 'green')
                }

                for level in [2, 5, 8]:  # Only include levels 1, 5, 7, 8
                    # Get the corresponding color for the level
                    color = traffic_conditions[level][1]

                    # Add annotation with a black text
                    fig.add_annotation(
                        xref='paper', yref='paper',
                        x=1.04, y=(level - 1) * 0.125,  # Adjust position
                        text=f"{traffic_conditions[level][0]}",
                        showarrow=False,
                        bgcolor=color,
                        font=dict(color='black'),  # Set font color to black
                        bordercolor='black',
                        borderwidth=1,
                        borderpad=3,
                        xanchor='center',
                    )

                # Return the graph object (figure) to be rendered in Dash
                return dcc.Graph(figure=fig), None

