import dash_bootstrap_components as dbc
import plotly.express as px
from dash import html, dcc
from dash.dependencies import Input, Output
from postgresql.db_functions import *

layout = html.Div([
    html.H3('Traffic Insights', className="text-center mb-5 mt-2"),
    
    # Row for correlation and trend charts
    dbc.Row([
        dbc.Col(
            dcc.Graph(id='correlation-chart', className="rounded shadow p-3 mb-4"),
            width=6
        ),
        dbc.Col(
            dcc.Graph(id='trend-chart', className="rounded shadow p-3 mb-4"),
            width=6
        )
    ], className="mb-4", style={'flex-wrap': 'wrap', 'justify-content': 'space-between'}),
    
    # Interval for updating insights
    dcc.Interval(id='interval-component-insights', interval=2*1000, n_intervals=0),
    
    # New row for 2 table insights
    html.Div([
        dbc.Row([
        dbc.Col(
            html.H3('Traffic Condition', className="text-center mb-4"),
            width=6,  
        ),
        dbc.Col(
            html.H3('Annual Traffic Lights', className="text-center mb-4"),
            width=6,  
        )
    ], justify="between", className="mb-4"),  # Justify the titles between columns

    # Row for two tables
    dbc.Row([
        dbc.Col(
           dcc.Graph(id='speed-graph', className="rounded shadow p-3 mb-4"),
           width=6,
        ),
        dbc.Col(
            dcc.Graph(id='trafficlights-graph', className="rounded shadow p-3 mb-4"),
            width=6,
        )
    ],  justify="between", className="mb-4"),
    ]),
    
    # New row for car population graph
    html.Div([
        html.H3('Car Population Insights', className="text-center mb-4"),
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(
                    id='filter-dropdown',
                    options=[
                        {'label': 'By Car Make', 'value': 'make'},
                        {'label': 'By Car CC', 'value': 'cc'}
                    ],
                    value='make',  # Default selection
                    clearable=False
                ),
                width=6
            ),
            dbc.Col(
                dcc.Dropdown(
                    id='year-dropdown',
                    options=[],  # Initially empty, will be populated in callback
                    value=None,  # Will be set to the max year later
                    clearable=False
                ),
                width=6
            )
        ], className="mb-4"),
        
        dcc.Graph(id='population-graph', className="rounded shadow p-3 mb-4")
    ])
])

# Define callbacks
def register_callbacks(app):
    @app.callback(
        Output('trend-chart', 'figure'),
        Input('interval-component-insights', 'n_intervals')
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
        Output('correlation-chart', 'figure'),
        Input('interval-component-insights', 'n_intervals')
    )
    def update_correlation_chart(n):
        df = fetch_vms_incident_correlation()
        fig = px.scatter(
            df, 
            x="incident_count", 
            y="vms_message", 
            size="incident_count", 
            title="Correlation Between VMS Messages and Incidents",
            labels={"incident_count": "Total Incidents", "vms_message": "VMS Message"},
            hover_data={"incident_count": False, "vms_message": False}
        )
        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>Total Incidents: %{x}<extra></extra>"
        )
        fig.update_layout(
            margin={"r":0,"t":50,"l":0,"b":0},
            title={'x':0.5, 'xanchor': 'center'},
            xaxis_title="Number of Incidents",
            yaxis_title="VMS Message",
            template="plotly_white",
            hovermode="closest"
        )
        return fig
