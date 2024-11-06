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
                width=4
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Traffic Jams", className="card-title"),
                        html.H2(id="jam-count", className="card-text text-white", style={'transition': 'all 0.5s ease'}),
                        html.H5(id="avg-speed", className="card-subtitle text-light mt-2", style={'transition': 'all 0.5s ease'})
                    ]),
                ], className="shadow p-3 mb-4 bg-warning text-white rounded"),
                width=4
            )
        ], className="mb-4"),

        # Row with Graphs: Incident Density Map and Trend Chart
        dbc.Row([
            dbc.Col(dcc.Graph(id='incident-density-map', className="rounded shadow p-3 mb-4"), width=4),
            dbc.Col(dcc.Graph(id='trend-chart', className="rounded shadow p-3 mb-4"), width=8)
        ], className="mb-4"),

        # Additional Row with Pie Chart, Speed Trend Chart, and Road Speed Performance
        dbc.Row([
            dbc.Col(dcc.Graph(id='pie-chart', className="rounded shadow p-3 mb-4"), width=4),
            dbc.Col(dcc.Graph(id='speed-trend-chart', className="rounded shadow p-3 mb-4"), width=4),
            dbc.Col(dcc.Graph(id='road-speed-performance', className="rounded shadow p-3 mb-4"), width=4)
        ], className="mb-4"),

        # Real-time VMS Messages List
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Latest Traffic Messages", className="card-title"),
                        html.Div(id="vms-messages", className="vms-list text-dark", style={"maxHeight": "300px", "overflowY": "scroll"})
                    ]),
                ], className="shadow p-3 mb-4 bg-light rounded"),
                width=6
            ),
            dbc.Col(dcc.Graph(id='traffic-heatmap', className="rounded shadow p-3 mb-4"), width=6)
        ], className="mb-4"),



        # Auto-refresh interval
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

    # Update traffic jam count and average speed
    @app.callback(
        Output('jam-count', 'children'),
        Output('avg-speed', 'children'),
        Input('interval-component-overview', 'n_intervals')
    )
    def update_traffic_jams(n):
        traffic_jam_stats = fetch_traffic_jams()
        jam_count = traffic_jam_stats.get('jam_count', 'N/A')
        avg_speed = traffic_jam_stats.get('avg_speed', 'N/A')
        return f"{jam_count}", f"Avg Speed: {avg_speed:.2f} km/h" if avg_speed != 'N/A' else "N/A"

    # Update VMS messages
    @app.callback(
        Output('vms-messages', 'children'),
        Input('interval-component-overview', 'n_intervals')
    )
    def update_vms_messages(n):
        messages = fetch_recent_vms_messages()
        return [html.P(f"{msg['timestamp']}: {msg['Message']}") for msg in messages]
        
        
        
    # Incident Density Map
    @app.callback(
        Output('incident-density-map', 'figure'),
        Input('interval-component-overview', 'n_intervals')
    )
    def update_density_map(n):
        df = fetch_incident_density()  # Should return a DataFrame with 'Latitude' and 'Longitude'
        fig = px.density_mapbox(
            df, 
            lat='Latitude', 
            lon='Longitude', 
            radius=10, 
            zoom=10, 
            center=dict(lat=1.3521, lon=103.8198)
        )
        fig.update_layout(
            mapbox_style="carto-positron",
            title={
                'text': "Incident Density Map",
                'y':0.9,  # Position title closer to the top
                'x':0.5,  # Center the title
                'xanchor': 'center',
                'yanchor': 'top'
            },
            margin={"r":0,"t":50,"l":0,"b":0}  # Increase top margin for title visibility
        )
        return fig



    # Speed Trend Chart
    @app.callback(
    Output('speed-trend-chart', 'figure'),
    Input('interval-component-overview', 'n_intervals')
    )
    def update_speed_trend_chart(n):
        df = fetch_speed_trend_data()
        fig = px.line(
            df,
            x="timestamp",
            y="average_speed",
            title="Speed Trend Over Time",
            labels={"timestamp": "Time", "average_speed": "Average Speed (km/h)"}
        )
        fig.update_layout(template="plotly_white", hovermode="x unified")
        return fig
    
    # Road Speed Performance Chart
    @app.callback(
    Output('road-speed-performance', 'figure'),
    Input('interval-component-overview', 'n_intervals')
    )
    def update_road_speed_performance_chart(n):
        df = fetch_road_speed_performance_data()
        fig = px.bar(
            df,
            x="RoadName",
            y="average_speed",
            title="Road Speed Performance",
            labels={"RoadName": "Road Name", "average_speed": "Average Speed (km/h)"}
        )
        fig.update_layout(template="plotly_white", xaxis_tickangle=-45)
        return fig



    @app.callback(
    Output('traffic-heatmap', 'figure'),
    Input('interval-component-overview', 'n_intervals')
    )
    def update_traffic_heatmap(n):
        df = fetch_traffic_heatmap_data()

        # Verify the dataframe structure to confirm required columns
        if not {'latitude', 'longitude', 'intensity'}.issubset(df.columns):
            print("DataFrame missing required columns: 'latitude', 'longitude', 'intensity'")
            return {}

        fig = px.density_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            z="intensity",
            radius=10,
            center=dict(lat=1.3521, lon=103.8198),  # Set to Singapore coordinates as a fallback
            zoom=10,
            mapbox_style="carto-positron"  # Alternative style that doesn’t require a Mapbox token
        )

        fig.update_layout(
            title="Traffic Heat Map",
            title_x=0.5,
            margin={"r":0, "t":50, "l":0, "b":0}
        )

        return fig

    # Update trend chart
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
            color_discrete_sequence=["#adb5bd"]
        )
        fig.update_traces(
            line=dict(color="#74c0fc"),
            fill='tozeroy',
            hovertemplate="Date: %{x}<br>Number of Incidents: %{y}<extra></extra>"
        )
        fig.update_layout(
            margin={"r":0, "t":50, "l":0, "b":0},
            title={'x':0.5, 'xanchor': 'center'},
            xaxis_title="Date",
            yaxis_title="Number of Incidents",
            template="plotly_white",
            hovermode="x unified",
            transition={'duration': 500}
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
