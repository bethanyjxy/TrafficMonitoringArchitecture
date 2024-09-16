import sys
import os

import psycopg2
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../config')))
from postgres_config import POSTGRES_DB
from postgresql.db_functions import *
import plotly.express as px
import pandas as pd
from dash import html, dcc, Dash,dash_table
from dash.dependencies import Input, Output, State
from flask import Flask, send_from_directory, render_template, Response
import folium
# Import blueprints
from routes.template_routes import live_traffic_blueprint, templates_blueprint


# Initialize Flask server
server = Flask(__name__)

# Initialize Dash app (Dash uses Flask under the hood)
app = Dash(__name__, server=server, url_base_pathname='/map/', suppress_callback_exceptions=True)

# Register blueprints
server.register_blueprint(live_traffic_blueprint)
server.register_blueprint(templates_blueprint) 

# Layout for Dash app
app.layout = html.Div([
    html.H3('Real-Time Live Traffic', className="text-center mb-4"),
     # Dropdown to select the table
    dcc.Dropdown(
        id='table-selector',style={"width": "50%", "margin-bottom": "20px"},
        options=[
            {'label': 'Incident Table', 'value': 'incident_table'},
            {'label': 'VMS Table', 'value': 'vms_table'},
            {'label': 'Camera Table', 'value': 'image_table'} 
        ],
        value='incident_table'  # Default table
    ),
    
    # Div to display map
    html.Div([
        # Div for the map
        html.Div(id='map-output',  style={'flex': '60%', 'padding': '10px', 'flex-shrink': '0'}),  # Map takes 60% width, doesn't shrink
        # Div for the table
        html.Div([
            html.H4('Recent Incidents', className="text-center mb-4"),  # Title for the table
            html.Div(id='incident-table')  # Table will be generated here
        ], style={'flex': '40%', 'padding': '10px', 'display': 'flex', 'flex-direction': 'column', 'flex-shrink': '0'}) 
    ], style={'flex-direction': 'row', 'width': '100%', 'overflow': 'hidden' }),

    # Auto-refresh every 10 seconds
    dcc.Interval(id='interval-component', interval=10*1000, n_intervals=0)
])
@app.callback(
    [Output('map-output', 'children'), Output('incident-table', 'children')],
    [Input('interval-component', 'n_intervals'), Input('table-selector', 'value')]
)

def update_map(n, selected_table):
    data = fetch_data_from_table(selected_table)
    df = pd.DataFrame(data)

    if selected_table == 'incident_table':
        # Ensure the data is available
        if df.empty:
            return html.P("No data available.")
        
        
        # Scatter map with incidents as points
        fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="type", hover_data=["message"],
                                color="type",  zoom=11, height=400,width=1000)

        # Set map style and marker behavior on hover
        fig.update_traces(marker=dict(size=10, sizemode='area'),  # Default marker size
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

        fig.update_traces(marker=dict(sizemode="diameter", size=10, opacity=0.7))
         # Create incident table to display recent incidents
        incident_table_component = dash_table.DataTable(
            id='incident-table',
            columns=[
                {"name": "Datetime", "id": "datetime_str"},
                {"name": "Incident", "id": "message"}

            ],
            data=df[[ "message", "datetime_str"]].to_dict('records'),
            style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
            style_cell={'textAlign': 'left', 'fontSize': 12, 'font-family': 'Arial', 'padding': '5px'},
            page_size=10  # Show 10 incidents per page
        )

        return dcc.Graph(figure=fig), incident_table_component
    
    elif selected_table == 'image_table':
        # canot use this cuz image not working 
        # if df.empty:
        #     return html.P("No data available.")
        
        # # Ensure ImageLink column exists and URLs are correct
        # if 'imagelink' not in df.columns or df['imagelink'].isnull().all():
        #     return html.P("No images available.")

        # # Scatter map with camera locations as points
        # fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="cameraid",
        #                         zoom=11, height=600, width=1200)

        # # Use `hovertemplate` to display the image on hover
        # fig.update_traces(
        #     marker=dict(size=10, sizemode='area'),
        #     hovertemplate=(
        #         "<b>Camera ID: %{hovertext}</b><br>"
        #         "<img src='%{customdata[0]}' width='200px' height='150px'><extra></extra>"
        #     ),
        #     customdata=df[['imagelink']].values,  # Ensure that `customdata` is a 2D array
        #     hoverinfo='text',
        #     hoverlabel=dict(bgcolor="white", font_size=16)
        # )

        # fig.update_layout(
        #     mapbox_style="open-street-map",
        #     mapbox=dict(center=dict(lat=1.3521, lon=103.8198), zoom=11),
        #     margin={"r": 0, "t": 0, "l": 0, "b": 0},
        # )
        fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="cameraid", zoom=11, height=400,width=1000)
        fig.update_traces(marker=dict(size=10, sizemode='area'),  # Default marker size
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
        fig.update_traces(marker=dict(sizemode="diameter", size=10, opacity=0.7))
        return dcc.Graph(figure=fig)
    

    elif selected_table == 'vms_table':
        # Ensure the data is available
        if df.empty:
            return html.P("No data available.")
        
        # Scatter map with VMS locations as points
        fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="message",
                                 zoom=11, height=400,width=1000)

        # Set map style and marker behavior on hover
        fig.update_traces(marker=dict(size=10, sizemode='area'),  # Default marker size
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
        fig.update_traces(marker=dict(sizemode="diameter", size=10, opacity=0.7))

        return dcc.Graph(figure=fig)


@server.route('/map/')
def render_map():
    return app.index()


# Run the Dash server
if __name__ == '__main__':
    server.run(debug=True, host='0.0.0.0', port=5000)