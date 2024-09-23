
import dash_bootstrap_components as dbc
from postgresql.db_functions import *
import plotly.express as px
import pandas as pd
from dash import html, dcc, Dash,dash_table
from dash.dependencies import Input, Output
from flask import Flask, render_template
# Import blueprints
from routes.template_routes import live_traffic_blueprint, templates_blueprint



# Initialize Flask server
server = Flask(__name__)

# Initialize Dash app (Dash uses Flask under the hood)
traffic_app = Dash(__name__, server=server, url_base_pathname='/map/', suppress_callback_exceptions=True)
overview_app = Dash(__name__, server=server, url_base_pathname='/overview/',
                    external_stylesheets=[dbc.themes.BOOTSTRAP])  # Ensure Bootstrap is loaded

# Register blueprints
server.register_blueprint(live_traffic_blueprint)
server.register_blueprint(templates_blueprint) 

# Layout for Dash app
traffic_app.layout = html.Div([
    # Page Title
    html.H3('Real-Time Live Traffic', className="text-center mb-4"),
    
    # Dropdown to select the table
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(
                id='table-selector',
                style={"width": "70%"},
                options=[
                    {'label': 'Incident Table', 'value': 'incident_table'},
                    {'label': 'VMS Table', 'value': 'vms_table'},
                    {'label': 'Camera Table', 'value': 'image_table'} 
                ],
                value='incident_table'  # Default table
            ),
            width=6,  # Dropdown takes half the width
            className="mb-4"  # Add some bottom margin for spacing
        )
    ], justify="center"),  # Center the dropdown
    
    # Div to display map and table
    dbc.Row([
        # Column for the map
        dbc.Col(
            html.Div(id='map-output', style={'padding': '10px'}),
            width=8,  # Map takes up more space
            className="shadow-sm p-3 mb-4 bg-white rounded"  # Add some styling and spacing
        ),
        # Column for the incident table
        dbc.Col([
            html.H4('Recent Incidents', className="text-center mb-4"),
            html.Div(id='incident-table')
        ], width=4, className="shadow-sm p-3 mb-4 bg-white rounded")  # Table takes up less space and has similar styling
    ], justify="space-between", style={'padding': '0 15px'}),  # Ensure consistent padding
    
    # Auto-refresh every 2 seconds
    dcc.Interval(id='interval-component', interval=2*1000, n_intervals=0)
], style={'max-width': '100%', 'margin': '0 auto', 'padding': '20px', 'overflow-x': 'hidden'})
@traffic_app.callback(
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
                                color="type",  zoom=11, height=600,width=1200)

        # Set map style and marker behavior on hover
        fig.update_traces(marker=dict(size=14, sizemode='area'),  # Default marker size
                          selector=dict(mode='markers'),
                          hoverinfo='text',
                          hoverlabel=dict(bgcolor="white", font_size=12, font_family="Arial"))

        # Use Mapbox open street map style
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=1.3521, lon=103.8198),  # Singapore coordinates
                zoom=11
            ),
            margin={"r":0,"t":0,"l":0,"b":0},  # Remove margins
            
        )
        
        df = df.sort_values(by=['incident_date', 'incident_time'], ascending=[False, False])  # Sort by date and time in descending order

        fig.update_traces(marker=dict(sizemode="diameter", size=12, opacity=0.7))
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
        traffic_images = fetch_recent_images()
        img_df = pd.DataFrame(traffic_images)
        fig = px.scatter_mapbox(img_df, lat="latitude", lon="longitude", hover_name="cameraid", zoom=11, height=400,width=1000)
        fig.update_traces(marker=dict(size=12, sizemode='area'),  # Default marker size
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
        fig.update_traces(marker=dict(sizemode="diameter", size=12, opacity=0.7))
        return dcc.Graph(figure=fig), None
    

    elif selected_table == 'vms_table':
        # Ensure the data is available
        if df.empty:
            return html.P("No data available.")
        
        # Scatter map with VMS locations as points
        fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", hover_name="message",
                                 zoom=11, height=600,width=1200)

        # Set map style and marker behavior on hover
        fig.update_traces(marker=dict(size=10, sizemode='area'),  # Default marker size
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


@server.route('/map/')
def render_map():
    return traffic_app.index()


# Traffic Overview Page Layout
overview_app.layout = html.Div([
    html.H3('Traffic Overview', className="text-center mb-5 mt-2"),

    html.Div([
        # First Row: Metrics Card and Pie Chart
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='pie-chart', className="rounded shadow p-3 mb-4"),
                width=6
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Span("Incidents Today: ", className="card-title", style={'font-size': '24px', 'font-weight': 'bold'}),
                        html.Span(id="incident-count", className="card-text ", style={'font-size': '24px', 'font-weight': 'bold'}),
                    ])
                ], className="shadow p-3 mb-4 bg-primary text-white rounded"),
                width=6
            )
            
        ], className="mb-4", style={'flex-wrap': 'wrap', 'justify-content': 'space-between' }),

        # Second Row: Correlation and Trend Charts
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

        # Auto-refresh every 10 seconds
        dcc.Interval(id='interval-component-overview', interval=2*1000, n_intervals=0)
    ], style={'overflow-x': 'hidden'})
]) 


@overview_app.callback(
    Output('incident-count', 'children'),
    Input('interval-component-overview', 'n_intervals')
)
def update_incident_count(n):
    # Fetch the number of incidents today
    incident_count = fetch_incident_count_today()
    # Return the formatted string for display in the card
    return f"{incident_count}"


# Callback to update the trend chart
@overview_app.callback(
    Output('trend-chart', 'figure'),
    [Input('interval-component-overview', 'n_intervals')]
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


@overview_app.callback(
    Output('correlation-chart', 'figure'),
    Input('interval-component-overview', 'n_intervals')
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

@overview_app.callback(
    Output('pie-chart', 'figure'),
    [Input('interval-component-overview', 'n_intervals')]
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


@server.route('/overview/')
def traffic_overview():
    return render_template('traffic_overview.html')
# Run the Dash server
if __name__ == '__main__':
    server.run(debug=True, host='0.0.0.0', port=5000)