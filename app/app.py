import dash_bootstrap_components as dbc
from dash import  Dash
from flask import Flask, render_template

# Import blueprints
from routes.template_routes import templates_blueprint
from routes.liveTraffic_routes import live_traffic_blueprint

# Initialize Flask server
server = Flask(__name__)

# Initialize Dash app (Dash uses Flask under the hood)
traffic_app = Dash(__name__, server=server, url_base_pathname='/map/',  external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
overview_app = Dash(__name__, server=server, url_base_pathname='/overview/',
                    external_stylesheets=[dbc.themes.BOOTSTRAP])  # Ensure Bootstrap is loaded

# Register blueprints
server.register_blueprint(live_traffic_blueprint)
server.register_blueprint(templates_blueprint)

# Import the layout and callbacks
from layout.overviewLayout import layout as overview_layout, register_callbacks as overview_callbacks
from layout.mapLayout import layout as map_layout, register_callbacks as map_callbacks

# Set the layouts
traffic_app.layout = map_layout
overview_app.layout = overview_layout

# Register callbacks
map_callbacks(traffic_app)
overview_callbacks(overview_app)

# Define routes for Dash apps
@server.route('/map/')
def render_map():
    return traffic_app.index()

@server.route('/overview/')
def traffic_overview():
    return overview_app.index()

if __name__ == '__main__':
    server.run(debug=True, host='0.0.0.0', port=5000)

