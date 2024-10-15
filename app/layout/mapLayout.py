# mapLayout.py
import dash_bootstrap_components as dbc
from postgresql.db_stream import *
import plotly.express as px
import pandas as pd
import numpy as np 
from dash import html,dcc,dash_table
from dash.dependencies import Input, Output
import plotly.graph_objects as go


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
                options=[],  # Will be populated dynamically
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
            className="shadow-sm p-3 mb-4 bg-white rounded" 
        )
    ], justify="center"),  # Center the map

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

# Define callback registration as a separate function
def register_callbacks(app):
    @app.callback(
        Output('location-row', 'style'),
        Input('table-selector', 'value')
    )
    def toggle_location_dropdown(selected_table):
        if selected_table == 'speedbands_table':
            return {'display': 'block'}  # Show the dropdown
        else:
            return {'display': 'none'}  # Hide the dropdown
    
    @app.callback(
    #Update Location dropdown selection 
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
    
    # Update Map   
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

            fig.update_traces(marker=dict(sizemode="diameter", size=12, opacity=0.7))
            
            return dcc.Graph(figure=fig)


        elif selected_table == 'image_table':
            image_df = fetch_recent_images()
            
            fig = px.scatter_mapbox(image_df, lat="latitude", lon="longitude", hover_name="cameraid",zoom=11, height=600, width=1200)
            fig.update_traces(
                    marker=dict(size=12, sizemode='area'),
                    selector=dict(mode='markers'),
                    hoverinfo='text',  # Set the hover info
                    hoverlabel=dict(bgcolor="white", font_size=16),
                    #hovertemplate="<b>CameraID:</b> %{customdata[0]}<br><b>Image Link:</b> <a href='%{customdata[1]}' target='_blank'>Open Image</a><br><img src='%{customdata[1]}' width='200'><br>"
                )
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
            # Ensure data is available
            if df.empty:
                return html.P("No data available."), None
            
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
                    marker=dict(size=10, sizemode='area'),  # Default marker size
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

                return dcc.Graph(figure=fig)  # Return the figure to be displayed in the output Div