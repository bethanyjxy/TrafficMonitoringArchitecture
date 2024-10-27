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
            style={'display':'block'},
        )
    ], justify="center"),  
    
    # Row for the speedband map
    dbc.Row([  
        dbc.Col(
            html.Div([
                dcc.Graph(id='speedband-map'),
            ]),
            width=12,  
            className="shadow-sm p-3 mb-4 bg-white rounded",
            id="speedband-map-row",
            style={'display': 'none'}, 
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


        # Sort data by 'incident_date' and 'incident_time' in descending order
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

    # Function to change from general map to Traffic flow Map or Camera Map
    @app.callback(
        [
            Output('general-map-row', 'style'),  # General map
            Output('speedband-map-row', 'style'),  # Speedband map
            Output('camera-map-row', 'style'),  # Camera map
            Output('image-container-row', 'style')  # Image container
        ],
        Input('table-selector', 'value')
        )
    def toggle_maps(selected_table):
        if selected_table == 'speedbands_table':
            # Show the speedband map & polylines image container
            return {'display': 'none'}, {'display': 'block'}, {'display': 'none'}, {'display': 'none'}
        elif selected_table == 'image_table':
            # Show the camera map & image container
            return {'display': 'none'}, {'display': 'none'}, {'display': 'block'}, {'display': 'block'}
        else:
            # Show the general map, hide speedband map, camera map & their containers
            return {'display': 'block'}, {'display': 'none'}, {'display': 'none'}, {'display': 'none'}

    # Function to display speedband map 
    @app.callback(
        Output('speedband-map', 'figure', allow_duplicate=True),
        Output('location-dropdown', 'value', allow_duplicate=True),  # Update dropdown value
        Input('location-dropdown', 'value'),  # Location from dropdown
        Input('speedband-map', 'clickData'),  # Click data from map
        prevent_initial_call=True
    )
    def update_speedband_map(selected_location, click_data):
        # Initialize the figure
        fig = go.Figure()

        # If click_data is available, use it to update selected_location
        if click_data:
            selected_location = click_data['points'][0]['hovertext']
        
        # Check if selected_location is None or empty (use dropdown to clear)
        if not selected_location:
            # Fetch all road locations if no location is selected
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
                marker=dict(size=10, sizemode='area', opacity=0.7), 
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

            return fig, None  # Reset dropdown value

        else:
            # Fetch speedband data for the selected location
            speed_df = fetch_speedband_location(selected_location)

            # Check if there is data for the selected location
            if speed_df.empty:
                # Handle case where no data is available for the selected location
                fig.update_layout(
                    title="No Data Available",
                    mapbox_style="open-street-map",
                    mapbox=dict(center=dict(lat=1.3521, lon=103.8198), zoom=11),
                    height=600,
                    width=1200,
                    margin={"r": 0, "t": 0, "l": 0, "b": 0}
                )
                return fig, selected_location

            # Prepare polylines for the selected location
            polylines = []
            for _, row in speed_df.iterrows():
                start_lat, start_lon = row['startlat'], row['startlon']
                end_lat, end_lon = row['endlat'], row['endlon']
                cog_lvl = row['speedband']

                # Validate coordinates before creating polylines
                if pd.notna(start_lat) and pd.notna(start_lon) and pd.notna(end_lat) and pd.notna(end_lon):
                    line_color = 'red' if cog_lvl <= 2 else 'orange' if cog_lvl <= 5 else 'green'

                    polylines.append({
                        "lat": [start_lat, end_lat],
                        "lon": [start_lon, end_lon],
                        "color": line_color,
                        "traffic_label": f"{'Heavy' if cog_lvl <= 2 else 'Moderate' if cog_lvl <= 5 else 'Light'} Traffic"
                    })

            # Add polylines to the figure
            for polyline in polylines:
                fig.add_trace(go.Scattermapbox(
                    lat=polyline["lat"],
                    lon=polyline["lon"],
                    mode='lines',
                    line=dict(width=8, color=polyline['color']),
                    hoverinfo='text',
                    hovertext=polyline['traffic_label'],
                    showlegend=False  # Remove legend for speedbands
                ))

            # Center the map based on the polyline locations
            lat_center = np.mean([p["lat"][0] for p in polylines])
            lon_center = np.mean([p["lon"][0] for p in polylines])

            fig.update_layout(
                mapbox_style="open-street-map",
                mapbox=dict(center=dict(lat=lat_center, lon=lon_center), zoom=16),
                height=600,
                width=1200,
                margin={"r": 0, "t": 0, "l": 0, "b": 0}
            )

            return fig, selected_location

    # Function to display Camera Image Map
    @app.callback(
        [Output('camera-map', 'figure'),        # Update the figure of the graph
        Output('image-container', 'children')],  # Update the image container
        [Input('table-selector', 'value'),      # Input to fetch data based on selected table
        Input('camera-map', 'clickData')]       # Listen for click events on the map
    )
    def update_camera_map(selected_table, click_data):
        # Fetch image table
        image_df = fetch_images_table()
        
        # Map camera_id to road name (if it exists) or camera_id itself
        image_df['road_name'] = image_df['camera_id'].apply(lambda x: camera_location_mapping.get(str(x), str(x)))

        # Create the figure
        fig = go.Figure(go.Scattermapbox(
            lat=image_df['latitude'],
            lon=image_df['longitude'],
            mode='markers',
            marker=dict(size=12,sizemode='diameter'),
            hoverinfo='text',
            text=image_df['road_name'],  # Show camera ID on hover
        ))

        # Set the map style
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(center=dict(lat=1.3521, lon=103.8198), zoom=11),
            height=600,  
            width=1000, 
            margin={"r":0,"t":0,"l":0,"b":0},
        )

        # Default image content when no camera is clicked
        image_content = html.Div([
            html.H3(f"Select a camera on the map", style={"color": "darkblue"}),
        ])

        # Check if there is click data
        if click_data is not None:
            # Extract the camera ID based on the clickData
            point_number = click_data['points'][0]['pointNumber']
            selected_camera = image_df.iloc[point_number]
            
            rd_name = selected_camera['road_name']
            image_src = selected_camera['image_url']
            
            # Create image content to show in the container
            image_content = html.Div([
                html.Img(src=image_src, style={"width": "85%"}),
                html.H4(f"View from {rd_name}", style={"color": "darkblue"}),
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
            
            
# Define the mapping of CameraID to Location
camera_location_mapping = {
    "1111": "TPE(PIE) - Exit 2 to Loyang Ave",
    "1112": "TPE(PIE) - Tampines Viaduct",
    "1113": "Tanah Merah Coast Road towards Changi",
    "1701": "CTE (AYE) - Moulmein Flyover LP448F",
    "1702": "CTE (AYE) - Braddell Flyover LP274F",
    "1703": "CTE (SLE) - Blk 22 St George's Road",
    "1704": "CTE (AYE) - Entrance from Chin Swee Road",
    "1705": "CTE (AYE) - Ang Mo Kio Ave 5 Flyover",
    "1706": "CTE (AYE) - Yio Chu Kang Flyover",
    "1707": "CTE (AYE) - Bukit Merah Flyover",
    "1709": "CTE (AYE) - Exit 6 to Bukit Timah Road",
    "1711": "CTE (AYE) - Ang Mo Kio Flyover",
    "2701": "Woodlands Causeway (Towards Johor)",
    "2702": "Woodlands Checkpoint",
    "2703": "BKE (PIE) - Chantek F/O",
    "2704": "BKE (Woodlands Checkpoint) - Woodlands F/O",
    "2705": "BKE (PIE) - Dairy Farm F/O",
    "2706": "Entrance from Mandai Rd (Towards Checkpoint)",
    "2707": "Exit 5 to KJE (towards PIE)",
    "2708": "Exit 5 to KJE (Towards Checkpoint)",
    "3702": "ECP (Changi) - Entrance from PIE",
    "3704": "ECP (Changi) - Entrance from KPE",
    "3705": "ECP (AYE) - Exit 2A to Changi Coast Road",
    "3793": "ECP (Changi) - Laguna Flyover",
    "3795": "ECP (City) - Marine Parade F/O",
    "3796": "ECP (Changi) - Tanjong Katong F/O",
    "3797": "ECP (City) - Tanjung Rhu",
    "3798": "ECP (Changi) - Benjamin Sheares Bridge",
    "4701": "AYE (City) - Alexander Road Exit",
    "4702": "AYE (Jurong) - Keppel Viaduct",
    "4703": "Tuas Second Link",
    "4704": "AYE (CTE) - Lower Delta Road F/O",
    "4705": "AYE (MCE) - Entrance from Yuan Ching Rd",
    "4706": "AYE (Jurong) - NUS Sch of Computing TID",
    "4707": "AYE (MCE) - Entrance from Jln Ahmad Ibrahim",
    "4708": "AYE (CTE) - ITE College West Dover TID",
    "4709": "Clementi Ave 6 Entrance",
    "4710": "AYE(Tuas) - Pandan Garden",
    "4712": "AYE(Tuas) - Tuas Ave 8 Exit",
    "4713": "Tuas Checkpoint",
    "4714": "AYE (Tuas) - Near West Coast Walk",
    "4716": "AYE (Tuas) - Entrance from Benoi Rd",
    "4798": "Sentosa Tower 1",
    "4799": "Sentosa Tower 2",
    "5794": "PIEE (Jurong) - Bedok North",
    "5795": "PIEE (Jurong) - Eunos F/O",
    "5797": "PIEE (Jurong) - Paya Lebar F/O",
    "5798": "PIEE (Jurong) - Kallang Sims Drive Blk 62",
    "5799": "PIEE (Changi) - Woodsville F/O",
    "6701": "PIEW (Changi) - Blk 65A Jln Tenteram, Kim Keat",
    "6703": "PIEW (Changi) - Blk 173 Toa Payoh Lorong 1",
    "6704": "PIEW (Jurong) - Mt Pleasant F/O",
    "6705": "PIEW (Changi) - Adam F/O Special pole",
    "6706": "PIEW (Changi) - BKE",
    "6708": "Nanyang Flyover (Towards Changi)",
    "6710": "Entrance from Jln Anak Bukit (Towards Changi)",
    "6711": "Entrance from ECP (Towards Jurong)",
    "6712": "Exit 27 to Clementi Ave 6",
    "6713": "Entrance From Simei Ave (Towards Jurong)",
    "6714": "Exit 35 to KJE (Towards Changi)",
    "6715": "Hong Kah Flyover (Towards Jurong)",
    "6716": "AYE Flyover",
    "7791": "TPE (PIE) - Upper Changi F/O",
    "7793": "TPE(PIE) - Entrance to PIE from Tampines Ave 10",
    "7794": "TPE(SLE) - TPE Exit KPE",
    "7795": "TPE(PIE) - Entrance from Tampines FO",
    "7796": "TPE(SLE) - On rooflp of Blk 189A Rivervale Drive 9",
    "7797": "TPE(PIE) - Seletar Flyover",
    "7798": "TPE(SLE) - LP790F (On SLE Flyover)",
    "8701": "KJE (PIE) - Choa Chu Kang West Flyover",
    "8702": "KJE (BKE) - Exit To BKE",
    "8704": "KJE (BKE) - Entrance From Choa Chu Kang Dr",
    "8706": "KJE (BKE) - Tengah Flyover",
    "9701": "SLE (TPE) - Lentor F/O",
    "9702": "SLE(TPE) - Thomson Flyover",
    "9703": "SLE(Woodlands) - Woodlands South Flyover",
    "9704": "SLE(TPE) - Ulu Sembawang Flyover",
    "9705": "SLE(TPE) - Beside Slip Road From Woodland Ave 2",
    "9706": "SLE(Woodlands) - Mandai Lake Flyover"
}