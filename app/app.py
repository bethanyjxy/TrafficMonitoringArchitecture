import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import psycopg2

# Initialize Dash app (Dash uses Flask under the hood)
app = dash.Dash(__name__)

# Define the layout of the app (this is the HTML structure)
app.layout = html.Div(children=[
    html.H1('Traffic Monitoring Dashboard'),
    
    # A simple display for testing
    html.Div(id='db-status', children='PostgreSQL Connection: Unknown'),

    # A button to check the PostgreSQL connection
    html.Button('Check PostgreSQL Connection', id='check-connection-button'),
    
    # A paragraph that will update with the connection status
    html.Div(id='connection-result')
])

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
    app.run_server(debug=True, host='0.0.0.0', port=5000)