import dash_bootstrap_components as dbc
import plotly.express as px
from dash import html, dcc
from dash.dependencies import Input, Output

from postgresql.db_stream import *

# Traffic Overview Layout
layout = html.Div([
    html.H3('Traffic Overview', className="text-center mb-5 mt-2"),
    dbc.Container([
        # Row with Metrics
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Incidents Today", className="card-title"),
                        html.H2(id="incident-count", className="card-text text-white", style={'transition': 'all 0.5s ease'}),
                    ]),
                ], className="shadow p-3 mb-4 bg-danger text-white rounded"),
                width=6
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Traffic Jams", className="card-title"),
                        html.H2(id="jam-count", className="card-text text-white", style={'transition': 'all 0.5s ease'}),
                        html.H5(id="avg-speed", className="card-subtitle text-light mt-2", style={'transition': 'all 0.5s ease'})
                    ]),
                ], className="shadow p-3 mb-4 bg-warning text-white rounded"),
                width=6
            )
        ], className="mb-4"),
        
        # Row with Graphs
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='pie-chart', className="rounded shadow p-3 mb-4"),
                width=6
            ),
            dbc.Col(
                dcc.Graph(id='trend-chart', className="rounded shadow p-3 mb-4"),
                width=6
            )
        ], className="mb-4"),
        
        dcc.Interval(id='interval-component-overview', interval=2*1000, n_intervals=0)
    ], fluid=True)
])

# Define callback registration function
def register_callbacks(app):
    @app.callback(
        Output('incident-count', 'children'),
        Input('interval-component-overview', 'n_intervals')
    )
    def update_incident_count(n):
        incident_count = fetch_incident_count_today()
        return f"{incident_count}"

    # Callback to update the jam count and average speed
    @app.callback(
        Output('jam-count', 'children'),
        Output('avg-speed', 'children'),
        Input('interval-component-overview', 'n_intervals')
    )
    def update_traffic_jams(n):
        traffic_jam_stats = fetch_traffic_jams()
        jam_count = traffic_jam_stats['jam_count']
        avg_speed = traffic_jam_stats['avg_speed']
        return f"{jam_count}", f"Avg Speed: {avg_speed:.2f} km/h" if avg_speed else "N/A"

    
    # Update trend chart without `animation_frame` for continuous data
    @app.callback(
        Output('trend-chart', 'figure'),
        Input('interval-component-overview', 'n_intervals')
    )
    def update_trend_chart(n):
        df = fetch_incidents_over_time()

        fig = px.area(
            df, 
            x="incident_date", 
            y="incident_count", 
            title="Incident Trends Over Time",
            labels={"incident_date": "Date", "incident_count": "Number of Incidents"},
            color_discrete_sequence=["#adb5bd"],  # Use a gradient color if desired
        )
        fig.update_traces(
            line=dict(color="#74c0fc"),  # Cyan line color
            fill='tozeroy',  # Fill the area to the zero line
            hovertemplate="Date: %{x}<br>Number of Incidents: %{y}<extra></extra>"
        )

        fig.update_layout(
            margin={"r":0, "t":50, "l":0, "b":0},
            title={'x':0.5, 'xanchor': 'center'},
            xaxis_title="Date",
            yaxis_title="Number of Incidents",
            template="plotly_white",
            hovermode="x unified",
            transition={'duration': 500}  # Smooth transition for each update
        )
        return fig


    # Update pie chart
    @app.callback(
        Output('pie-chart', 'figure'),
        Input('interval-component-overview', 'n_intervals')
    )
    def update_pie_chart(n):
        df = fetch_vehicle_type_incidents()
        fig = px.pie(
            df, 
            values="vehicle_count", 
            names="vehicle_type", 
            title="Incidents by Vehicle Type",
            labels={"vehicle_count": "Vehicles", "vehicle_type": "Type"}
        )
        fig.update_traces(
            hovertemplate="<b>%{label}</b><br>Vehicles: %{value}<extra></extra>"
        )
        return fig
