import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import psycopg2
from flask import Flask, send_from_directory, render_template
import folium

# Initialize Flask server
server = Flask(__name__)

# Initialize Dash app (Dash uses Flask under the hood)
app = dash.Dash(__name__, server=server, url_base_pathname='/dashboard/', suppress_callback_exceptions=True)

# Define the layout of the app
app.layout = html.Div(children=[
    html.H1('Welcome to the Traffic Monitoring Dashboard App'),
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

# Route to serve static files
@server.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

# Route to landing page
@server.route('/')
def serve_main():
    return send_from_directory('templates', 'main.html')

@server.route('/live_traffic')
def live_traffic():
    # Example incidents data
    incidents = [
        {"lat": 1.3521, "lon": 103.8198, "desc": "Accident at location 1"},
        {"lat": 1.365, "lon": 103.8398, "desc": "Heavy traffic at location 2"},
    ]

    # Create a Folium map centered on Singapore
    map_obj = folium.Map(location=[1.3521, 103.8198], zoom_start=12)

    # Add markers dynamically from incidents
    for incident in incidents:
        folium.Marker([incident["lat"], incident["lon"]], popup=incident["desc"]).add_to(map_obj)

    # Render map as HTML
    map_html = map_obj._repr_html_()
    return render_template('liveTraffic.html', map_html=map_html)

# Route to check PostgreSQL connection
@server.route('/check-connection')
def check_connection():
    conn = connect_db()
    if isinstance(conn, str):
        return conn
    else:
        return "Successfully connected to PostgreSQL!"

# PostgreSQL connection function
def connect_db():
    try:
        connection = psycopg2.connect(
            database="traffic_db",
            user="traffic_admin",
            password="traffic_pass",
            host="postgres",  # This is the service name in Docker Compose
            port="5432"
        )
        return connection
    except Exception as e:
        return str(e)

# Callback to check database connection when the button is clicked
@app.callback(
    Output('connection-result', 'children'),
    [Input('check-connection-button', 'n_clicks')]
)
def update_db_status(n_clicks):
    if n_clicks is None:
        return ""
    conn = connect_db()
    if isinstance(conn, str):
        return f"Connection failed: {conn}"
    else:
        return "Successfully connected to PostgreSQL!"

# Run the Dash server
if __name__ == '__main__':
    server.run(debug=True, host='0.0.0.0', port=5000)