# mapLayout.py
import dash_bootstrap_components as dbc
from postgresql.db_functions import *
import plotly.express as px
import pandas as pd
import numpy as np 
from dash import html,dcc,dash_table
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from dash import callback_context


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
            width=6,  # Dropdown takes half the width
            className="mb-4"  # Add some bottom margin for spacing
        )
    ], justify="left"),  # Center the dropdown
    
    # Row for the map
    dbc.Row([
        # Column for the map
        dbc.Col(
            html.Div(id='map-output', style={'padding': '10px'}),
            width=12,  # Use the full width for the map
            className="shadow-sm p-3 mb-4 bg-white rounded"  # Add some styling and spacing
        )
    ], justify="center"),  # Center the map

    # Row for the incident table below the map
    dbc.Row([
        # Column for the incident table
        dbc.Col([
            html.H3('Recent Incidents', className="text-left mb-4"),
            html.Div(id='incident-table')
        ], width=12, className="shadow-sm p-3 mb-4 bg-white rounded")  # Table takes up full width and has similar styling
    ], justify="center"),  # Center the table
    
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

        elif selected_table == 'speedbands_table':
            if df.empty:
                return html.P("No data available.")

            cog_data = []  # List to collect heatmap data

            # Loop over each row in the dataframe
            for i, row in df.iterrows():
                start_lat = row['startlat']
                start_lon = row['startlon']
                end_lat = row['endlat']
                end_lon = row['endlon']
                cog_lvl = row['speedband']  # Get the congestion level

                if start_lat and start_lon and end_lat and end_lon:
                    num_points = 10
                    latitudes = np.linspace(start_lat, end_lat, num_points)
                    longitudes = np.linspace(start_lon, end_lon, num_points)

                    # Collect heatmap points
                    for lat, lon in zip(latitudes, longitudes):
                        if cog_lvl >= 1 and cog_lvl <= 8:
                            cog_data.append((lat, lon, cog_lvl))

            if cog_data:
                lat, lon, intensity = zip(*cog_data)

                # Define custom colorscale for levels 1 to 8
                custom_colorscale = [
                    [0, 'red'],     # 0 < 9
                    [0.125, 'red'],     # 10 < 19
                    [0.25, 'red'],   # 20 < 29
                    [0.375, 'orange'],  # 30 < 39
                    [0.5, 'orange'],    # 40 < 49
                    [0.625, 'orange'],  # 50 < 59
                    [0.75, 'green'],    # 60 < 69
                    [0.875, 'green'],   # 70 or more
                    [1, 'green']
                ]

                fig = go.Figure(data=go.Densitymapbox(
                    lat=lat,
                    lon=lon,
                    z=intensity,
                    colorscale=custom_colorscale,
                    radius=5,
                    zmin=1,
                    zmax=8
                ))

                fig.update_layout(
                    mapbox_style="open-street-map",
                    mapbox=dict(center=dict(lat=np.mean(lat), lon=np.mean(lon)), zoom=11),
                    height=600,
                    width=1200,
                )

                # Add annotations for congestion levels
                traffic_conditions = {2: ("Heavy", 'red'), 5: ("Moderate", 'orange'), 8: ("Light", 'green')}
                for level in [2, 5, 8]:
                    fig.add_annotation(
                        xref='paper', yref='paper',
                        x=1.04, y=(level - 1) * 0.125,
                        text=f"{traffic_conditions[level][0]}",
                        showarrow=False,
                        bgcolor=traffic_conditions[level][1],
                        font=dict(color='black'),
                        bordercolor='black',
                        borderwidth=1,
                        borderpad=3,
                        xanchor='center',
                    )

                return dcc.Graph(figure=fig), None
