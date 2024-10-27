import dash_bootstrap_components as dbc
import dash_core_components as dcc
import plotly.express as px 
from dash import html, dcc, callback_context
from dash.dependencies import Input, Output
from postgresql.db_batch import *

layout = html.Div([
    html.H3('Traffic Insights', className="text-center mb-5 mt-2"),
    
    # First Row
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
    dcc.Interval(id='interval-component-insights', interval=300*1000, n_intervals=0),
    
    # 2nd Row
    html.Div([
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
    
    # 3rd Row
    html.Div([
        html.H3('Traffic Flow Prediction', className="text-center mb-4"),
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(
                id='road-name-dropdown',
                options=[],
                placeholder="Select a road name",
                clearable=False,
                className="mb-4"
            ),
            width=12,
        )
    ]),
    dbc.Row([
        dbc.Col(
            dcc.Graph(id='average-speed-graph', className="rounded shadow p-3 mb-4"),
            width=12,
        )
    ], justify="between", className="mb-4"),
    ]),

    
    # 4th row
    html.Div([
    html.H3('Vehicle Population', className="text-center mb-4"),
    
    dbc.Row([
        dbc.Col(
                dcc.Dropdown(
                    id='filter-dropdown',
                    options=[
                        {'label': 'Manufacturer', 'value': 'make'},
                        {'label': 'Credit Rating', 'value': 'cc'}
                    ],
                    value='make',  # Default selection
                    clearable= False,
                    className="mb-3"  
                ),
                width=6
            ),
            dbc.Col(
                dcc.Dropdown(
                    id='type-dropdown',
                    options=[],  
                    value=None, 
                    clearable = True,
                    className="mb-3" 
                ),
                width=6
            ),
    ]),
    
    dbc.Row([  
        dbc.Col(
            dcc.Graph(id='population-graph', className="rounded shadow p-3 mb-4"),  
            width=12
        ),
    ]),
    dbc.Row([
        dbc.Col(
            dbc.ButtonGroup(
                [
                    dbc.Button("Cars", id='car-button', color='primary', className='rounded', n_clicks=1),  # Default selected
                    dbc.Button("Motorcycles", id='motorcycle-button', color='secondary', className='mx-2 rounded')
                ],
                className='d-flex justify-content-center'  
            ),
            width={'size': 12, 'offset': 0} 
        )
    ], className="mb-4"),
])
])

# Define callbacks
def register_callbacks(app):
    @app.callback(
        Output('trend-chart', 'figure'),
        Input('interval-component-insights', 'n_intervals')
    )
    def update_trend_chart(n):
        # Fetch data from the database (fetch_report_incident already gets date and result)
        df = fetch_report_incident()

        # Convert the date column to a datetime format if it's not already
        df['date'] = pd.to_datetime(df['date'])

        # Get the current month name
        current_month = datetime.now().strftime('%B')

        # Plot the line chart with dates on the x-axis
        fig = px.bar(
            df, 
            x="date", 
            y="result", 
            title="Daily Incident Trends",  # Title reflects the current month
            labels={"date": "Date", "result": "Number of Incidents"}
        )

        # Customize hover template to show the date and number of incidents
        fig.update_traces(
            hovertemplate="Date: %{x}<br>Number of Incidents: %{y}<extra></extra>"
        )

        # Update the layout
        fig.update_layout(
            margin={"r": 0, "t": 50, "l": 0, "b": 0},
            title={'x': 0.5, 'xanchor': 'center'},
            xaxis_title="Date",
            yaxis_title="Number of Incidents",
            template="simple_white",
            hovermode="x unified",
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
    @app.callback(
        Output('trafficlights-graph', 'figure'),
        Input('interval-component-insights', 'n_intervals')
    )
    def update_trafficlight_graph(n): 
        df = fetch_data_from_table('annual_traffic_lights')
        # Create a bar chart
        fig = px.bar(
            df, 
            x="year", 
            y="traffic_lights", 
            title="Annual Traffic Light Installations",
            labels={"year": "Year", "traffic_lights": "Number of Traffic Lights"},
            text="traffic_lights"
        )

        # Update the layout and formatting
        fig.update_layout(
            margin={"r":0,"t":50,"l":0,"b":0},
            title={'x':0.5, 'xanchor': 'center'},
            xaxis_title="Year",
            yaxis_title="Number of Traffic Lights",
            template="plotly_white"
        )

        return fig
    
    @app.callback(
    Output('speed-graph', 'figure'),
    Input('interval-component-insights', 'n_intervals')
    )
    def update_road_conditions_graph(n):  
        df = fetch_data_from_table('road_traffic_condition')
        
         # Create a line chart 
        fig = px.line(
            df,
            x="year",
            y=["ave_speed_expressway", "ave_speed_arterial_roads"],
            title="Annual Road Traffic Conditions",
            labels={
                "year": "Year",
                "value": "Average Speed (km/h)",
                "variable": "Road Type"  
            }
        )

        fig.for_each_trace(lambda t: t.update(name="Expressway" if "ave_speed_expressway" in t.name else "Arterial Roads"))

        # Update layout and formatting
        fig.update_layout(
            margin={"r": 0, "t": 50, "l": 0, "b": 0},
            title={'x': 0.5, 'xanchor': 'center'},
            xaxis_title="Year",
            yaxis_title="Average Speed (km/h)",
            template="plotly_white"
        )

        return fig
    
    @app.callback(
    Output('population-graph', 'figure'),
        [Input('car-button', 'n_clicks'),
        Input('motorcycle-button', 'n_clicks'),
        Input('filter-dropdown', 'value'),
        Input('type-dropdown', 'value')]
    )
    def update_population_graph(car_clicks, motorcycle_clicks, filter_type, selected_type):
        # Use callback_context to determine which button was clicked
        ctx = callback_context

        # Set default click counts to 0 if they are None
        car_clicks = car_clicks or 0
        motorcycle_clicks = motorcycle_clicks or 0

        # Determine if Cars or Motorcycles is selected
        if ctx.triggered:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]  
            selected_category = 'cars' if button_id == 'car-button' else 'motorcycles'
        else:
            selected_category = 'cars'  # Default to cars if nothing triggered

        selected_category = 'cars' if car_clicks > motorcycle_clicks else 'motorcycles'
        
        # Get the overall population
        if selected_type is None:
            df = fetch_population_year_table(selected_category, filter_type)  
            if filter_type == 'make':
                name = 'Manufacturer'
            else:
                name = 'Credit Ratings'
            
            # Create a bar chart for overall population
            fig = px.bar(
                df,
                x="year",
                y="total_number",  
                title=f"Overall {selected_category.capitalize()} Population Per Year Based On {name}",
                labels={"year": "Year", "total_number": f"Number of {selected_category.capitalize()}s"},  
                text="total_number"
            )
        else:
            # Fetch data based on filter type and selection
            if filter_type == 'cc':
                df = fetch_population_cc_table(selected_category, selected_type)  
            else:
                df = fetch_population_make_table(selected_category, selected_type) 

            # Create a bar chart based on the selected filter
            fig = px.bar(
                df,
                x="year",
                y="total_number",
                title=f"{selected_category.capitalize()} Population Based On {selected_type}",
                labels={"year": "Year", "total_number": f"Number of {selected_category.capitalize()}s"},
                text="total_number"
            )

        # Update layout and formatting
        fig.update_layout(
            margin={"r": 0, "t": 50, "l": 0, "b": 0},
            title={'x': 0.5, 'xanchor': 'center'},
            xaxis_title="Year",
            yaxis_title=f"Number of {selected_category.capitalize()}s",
            template="plotly_white"
        )
        
        return fig
            
    @app.callback(
        Output('type-dropdown', 'options'),
        [Input('filter-dropdown', 'value'),
        Input('car-button', 'n_clicks'),
        Input('motorcycle-button', 'n_clicks')]
    )
    def update_type_dropdown(filter_type, car_clicks, motorcycle_clicks):
        # Use callback_context to determine which button was clicked
        ctx = callback_context

        # Set default click counts to 0 if they are None
        car_clicks = car_clicks or 0
        motorcycle_clicks = motorcycle_clicks or 0

        # Determine selected category (cars or motorcycles)
        if ctx.triggered:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
            selected_category = 'cars' if button_id == 'car-button' else 'motorcycles'
        else:
            selected_category = 'cars'  # Default to cars if nothing triggered

        # Fetch the data based on the selected filter type and category
        if filter_type == 'cc':
            df = fetch_unique_type_table(selected_category, 'cc')
        else:
            df = fetch_unique_type_table(selected_category, 'make')
        
        options = [{'label': str(car_type), 'value': car_type} for car_type in sorted(df)]

        return options
    
# Callback to update button styles based on selection
    @app.callback(
        [Output('car-button', 'color'),
        Output('motorcycle-button', 'color')],
        [Input('car-button', 'n_clicks'),
        Input('motorcycle-button', 'n_clicks')]
    )
    def update_button_colors(car_clicks, motorcycle_clicks):
        
    # Determine which button was clicked
        triggered = callback_context.triggered[0]['prop_id'].split('.')[0]

        if triggered == 'car-button':
            return 'primary', 'secondary'  # Cars button selected
        elif triggered == 'motorcycle-button':
            return 'secondary', 'primary'  # Motorcycles button selected
        else:
            return 'primary', 'secondary'  # Default to Cars

    @app.callback(
        [
            Output('road-name-dropdown', 'options'), 
            Output('average-speed-graph', 'figure')   
        ],
        Input('road-name-dropdown', 'value')         
    )
    def update_dropdown_and_graph(selected_road_name):
        road_names_list = fetch_unique_location()
        road_names_options = road_names_list

        # Set default to "ADIS ROAD" if no road is selected initially
        if selected_road_name is None:
            selected_road_name = "ADIS ROAD"

        df = fetch_average_speedband(selected_road_name)

        if df.empty:
            return road_names_options, px.scatter()  

        color_map = {
            "Heavy congestion": "red",
            "Moderate congestion": "orange",
            "Light to moderate congestion": "yellow",
            "Light congestion": "green",
            "Unknown": "gray"
        }

        # Create an ordered categorical type for congestion levels
        congestion_order = ["Light congestion", "Light to moderate congestion", "Moderate congestion", "Heavy congestion", "Unknown"]
        df['speedband_description'] = pd.Categorical(df['speedband_description'], categories=congestion_order, ordered=True)
        df = df.sort_values('speedband_description')  

        # Create a new column for colors based on congestion levels
        df['marker_color'] = df['speedband_description'].map(color_map)

        # Create scatter plot
        fig = px.scatter(
            df,
            x='hour_of_day',
            y='speedband_description', 
            title=f"Average Speedband for {selected_road_name}",
            hover_data={"speedband_description": True, "hour_of_day": True}, 
            custom_data=["speedband_description", "average_speedband"]
        )

        # Update the traces to use hovertemplate
        fig.update_traces(
            marker=dict(size=15, color=df['marker_color'], line=dict(width=1)),
            hovertemplate="<b>Hour of Day</b>: %{x}:00<br>"  
                        "<b>Average Speedband</b>: %{customdata[1]:.2f}<br>" 
                        "<b>Congestion Level</b>: %{customdata[0]}<br>"  
                        "<extra></extra>"  # Remove the default hover box extra info
        )

        # Update x-axis for hour of the day
        fig.update_xaxes(tickmode='array', tickvals=list(range(24)), ticktext=list(range(24)))

        # Adjust the layout and axes labels
        fig.update_layout(
            margin={"r": 0, "t": 50, "l": 0, "b": 0},
            title={'x': 0.5, 'xanchor': 'center'},
            xaxis_title="Hour of the Day",
            yaxis_title="Traffic Congestion Level" 
        )

        return road_names_options, fig