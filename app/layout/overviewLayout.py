import dash_bootstrap_components as dbc
import plotly.express as px
from dash import html, dcc
from dash.dependencies import Input, Output

from postgresql.db_functions import *


# Traffic Overview Layout
layout = html.Div([  
    html.H3('Traffic Overview', className="text-center mb-5 mt-2"),
    html.Div([
        # First Row: Metrics Card and Pie Chart
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Span("Incidents Today: ", className="card-title", style={'font-size': '24px', 'font-weight': 'bold'}),
                        html.Span(id="incident-count", className="card-text ", style={'font-size': '24px', 'font-weight': 'bold'}),
                    ])
                ], className="shadow p-3 mb-4 bg-primary text-white rounded"),
                width=6
            )
        ], className="mb-4", style={'flex-wrap': 'wrap', 'justify-content': 'space-between'}),
        
        # Second Row: Correlation and Trend Charts
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='pie-chart', className="rounded shadow p-3 mb-4"),
                width=6
            ),
            dbc.Col(
                dcc.Graph(id='trend-chart', className="rounded shadow p-3 mb-4"),
                width=6
            )
        ], className="mb-4", style={'flex-wrap': 'wrap', 'justify-content': 'space-between'}),
        dcc.Interval(id='interval-component-overview', interval=2*1000, n_intervals=0)  # Auto-refresh every 10 seconds
    ], style={'overflow-x': 'hidden'})
])

# Define callback registration function
def register_callbacks(app):
    @app.callback(
        Output('incident-count', 'children'),
        Input('interval-component-overview', 'n_intervals')
    )
    def update_incident_count(n):
        # Fetch the number of incidents today
        incident_count = fetch_incident_count_today()
        return f"{incident_count}"
    # Callback to update the trend chart
    @app.callback(
        Output('trend-chart', 'figure'),
        Input('interval-component-overview', 'n_intervals')
    )
    def update_trend_chart(n):
        df = fetch_incidents_over_time()
        fig = px.line(
            df, 
            x="incident_date", 
            y="incident_count", 
            title="Incident Trends Over Time",
            labels={"incident_date": "Date", "incident_count": "Number of Incidents"}
        )

        fig.update_traces(
            hovertemplate="Date: %{x}<br>Number of Incidents: %{y}<extra></extra>"
        )

        fig.update_layout(
            margin={"r":0,"t":50,"l":0,"b":0},
            title={'x':0.5, 'xanchor': 'center'},
            xaxis_title="Date",
            yaxis_title="Number of Incidents",
            template="simple_white",
            hovermode="x unified"
        )
        return fig



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
