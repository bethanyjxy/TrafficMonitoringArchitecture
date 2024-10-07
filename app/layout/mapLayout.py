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
    html.H3('Real-Time Live Traffic', className="text-left mb-4"),
    
    # Dropdown to select the table
     dbc.Row([
        dbc.Col(
            dcc.Dropdown(
                id='table-selector',
                style={"width": "70%"},
                options=[  
                    {'label': 'Traffic Incident', 'value': 'incident_table'},
                    {'label': 'Traffic Flow', 'value': 'speedbands_table'},
                    {'label': 'Traffic Authorities Message', 'value': 'vms_table'},
                    {'label': 'Camera Location', 'value': 'image_table'}
                ],
                value='incident_table'  # Default table
            ),
            width=6, 
            className="mb-4"  
        )
    ], justify="left"), 
    
    # Row for the map
    dbc.Row([
        dbc.Col(
            html.Div(id='map-output', style={'padding': '10px'}),
            width=12,  # Use the full width for the map
            className="shadow-sm p-3 mb-4 bg-white rounded"  # Add some styling and spacing
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
        [Output('map-output', 'children'), Output('incident-table', 'children')],
        [Input('interval-component', 'n_intervals'), Input('table-selector', 'value')]
    )
    def update_map(n, selected_table):
        data = fetch_data_from_table(selected_table) 
            
        data = fetch_data_from_table(selected_table)
        df = pd.DataFrame(data)

        if selected_table == 'incident_table':
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
            fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="location", zoom=11, height=600, width=1200)
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

            polylines = []  # List to collect polyline data

            # Loop over each row in the dataframe
            for i, row in df.iterrows():
                start_lat = row['startlat']
                start_lon = row['startlon']
                end_lat = row['endlat']
                end_lon = row['endlon']
                cog_lvl = row['speedband']  # Get the congestion level

                if start_lat and start_lon and end_lat and end_lon:
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

            fig = go.Figure()

            # Add polylines to the map, colored by congestion level with appropriate traffic labels
            for polyline in polylines:
                fig.add_trace(go.Scattermapbox(
                    lat=polyline["lat"],
                    lon=polyline["lon"],
                    mode='lines',
                    line=dict(width=3, color=polyline['color']),  # Line color based on congestion level
                    name=polyline['traffic_label'],  # Dynamic label based on traffic condition
                    hoverinfo='text',  # Show custom hover text
                    hovertext=polyline['traffic_label'],  # Full display of traffic label
                    showlegend=False
                ))

            fig.update_layout(
                mapbox_style="open-street-map",
                mapbox=dict(
                    center=dict(lat=np.mean([p["lat"][0] for p in polylines]), lon=np.mean([p["lon"][0] for p in polylines])),
                    zoom=15,
                ),
                height=600,  
                width=1200, 
                margin={"r": 0, "t": 0, "l": 0, "b": 0}  # Remove margins
            )

            return dcc.Graph(figure=fig), None



