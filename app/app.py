import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../config')))
from postgres_config import POSTGRES_DB
from postgresql.db_functions import check_db_connection

from dash import html, dcc, Dash
from dash.dependencies import Input, Output
from flask import Flask, send_from_directory, render_template
import folium
# Import blueprints
from routes.template_routes import live_traffic_blueprint, templates_blueprint


# Initialize Flask server
server = Flask(__name__)

# Initialize Dash app (Dash uses Flask under the hood)
app = Dash(__name__, server=server, url_base_pathname='/dashboard/', suppress_callback_exceptions=True)

# Register blueprints
server.register_blueprint(live_traffic_blueprint)
server.register_blueprint(templates_blueprint) 


# Define Dash layout
app.layout = html.Div(children=[
    dcc.Tabs([
        dcc.Tab(label='Traffic Overview', children=[
            html.H2('Traffic Overview'),
            html.Iframe(id='traffic_overview', src='/trafficOverview', width='100%', height='600')
        ]),
        dcc.Tab(label='Live Traffic', children=[
            html.H2('Live Traffic Map'),
            html.Iframe(id='live-traffic-map', src='/liveTraffic', width='100%', height='600')
        ]),
        dcc.Tab(label='Traffic Prediction', children=[
            html.H2('Traffic Prediction'),
            html.Iframe(id='traffic_prediction', src='/trafficPrediction', width='100%', height='600')
        ]),
    ]),
])


# Callback to check database connection when the button is clicked
@app.callback(
    Output('connection-result', 'children'),
    [Input('check-connection-button', 'n_clicks')]
)
def update_db_status(n_clicks):
    if n_clicks is None:
        return ""
    status = check_db_connection()  # Use the check_db_connection function from db_functions
    return status

# Run the Dash server
if __name__ == '__main__':
    server.run(debug=True, host='0.0.0.0', port=5000)

