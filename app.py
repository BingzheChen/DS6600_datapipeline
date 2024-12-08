import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
from datapipeline import DataPipeline
import plotly.figure_factory as ff
import plotly.graph_objs as go

# Connect to the PostgreSQL database
dp = DataPipeline()
# server, engine = dp.connect_to_mysql(pw=dp.MYSQL_PASSWORD, host='127.0.0.1')

# # Load the data from the database
# my_query = "SELECT * FROM hourlydata"
# hourly_data = pd.read_sql_query(my_query, con=engine)

# my_query = "SELECT * FROM dailydata"
# daily_data = pd.read_sql_query(my_query, con=engine)

# my_query = "SELECT * FROM constants"
# constants = pd.read_sql_query(my_query, con=engine)

hourly_data = pd.read_csv('hourlydata.csv')
daily_data = pd.read_csv('dailydata.csv')
constants = pd.read_csv('constants.csv')

# Dropdown options for cities and variables
city_options = [{'label': city, 'value': city} for city in constants['name'].unique()]
variable_options = [
    {'label': 'Temperature', 'value': 'temperature'},
    {'label': 'Windspeed', 'value': 'windspeed'},
    {'label': 'Precipitation', 'value': 'precipitation'}
] + [{'label': col.capitalize(), 'value': col} for col in daily_data.columns if col not in [
    'name', 'date', 'preciptype', 'severerisk', 'sunrise', 'sunset', 'conditions', 'description', 'source',
    'tempmax', 'tempmin', 'temp', 'feelslikemax', 'feelslikemin', 'feelslike',  # Part of 'Temperature'
    'windspeed', 'windspeedmax', 'windspeedmin', 'windspeedmean',  # Part of 'Windspeed'
    'precip', 'precipprob'  # Part of 'Precipitation'
]]

# Define the app
app = dash.Dash(__name__, external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])

# App layout
app.layout = html.Div([
    html.H1("What's the weather like?", style={'text-align': 'center'}),
    html.Div([
        # City dropdown
        html.Div([
            html.Label('Select a city:'),
            dcc.Dropdown(id='city-dropdown', options=city_options, value=city_options[0]['value'])
        ], style={'margin-bottom': '20px'}),

        # Starting date dropdown
        html.Div([
            html.Label('Select starting date:'),
            dcc.DatePickerSingle(
                id='start-date',
                min_date_allowed='2023-11-01',
                max_date_allowed='2024-10-31',
                initial_visible_month='2023-11-01',
                date='2023-11-01'
            )
        ], style={'margin-bottom': '20px'}),

        # Ending date dropdown
        html.Div([
            html.Label('Select ending date:'),
            dcc.DatePickerSingle(
                id='end-date',
                min_date_allowed='2023-11-01',
                max_date_allowed='2024-10-31',
                initial_visible_month='2023-11-01',
                date='2023-11-01'
            )
        ], style={'margin-bottom': '20px'}),

        # Current weather section
        html.Div([
            html.Label('Current Weather:'),
            html.Button('Get Current Weather', id='current-weather-button', n_clicks=0),
            html.Div(id='current-weather-output', style={'padding': '10px', 'margin-top': '10px'})
        ], style={'margin-bottom': '20px'})
    ], style={'width': '25%', 'float': 'left', 'padding': '10px'}),

    html.Div([
        # Tabs for displaying data and analysis
        dcc.Tabs([
            # Tab 1: City Info
            dcc.Tab(label='City Info', children=[
                html.Div(id='city-info-table', style={'padding': '20px'})
            ]),

            # Tab 2: Basic Weather Data
            dcc.Tab(label='Basic Weather Data', children=[
                html.Div([
                    dcc.Dropdown(id='variable-dropdown', options=variable_options, value=variable_options[0]['value'])
                ], style={'width': '50%', 'margin-bottom': '20px'}),
                html.Div(id='basic-weather-table', style={'padding': '20px'}),
                dcc.Graph(id='basic-weather-graph')
            ]),

            # Tab 3: Hourly Weather Data
            dcc.Tab(label='Hourly Weather Data', children=[
                dcc.Graph(id='temp-pressure-graph'),
                dcc.Graph(id='wind-heatmap-graph')
            ]),

            # Tab 4: Overall Analysis
            dcc.Tab(label='Overall Analysis', children=[
                html.Div(id='overall-analysis', style={'padding': '20px'})
            ]),

            # Tab 5: City Comparison
            dcc.Tab(label='City Comparison', children=[
                html.Div([
                    html.Label('Select another city to compare:'),
                    dcc.Dropdown(id='comparison-city-dropdown', options=city_options, value=city_options[0]['value'])
                ], style={'width': '50%', 'margin-bottom': '20px'}),
                html.Div(id='comparison-table', style={'padding': '20px'}),
                # html.Div(id='comparison-table'),
                dcc.Graph(id='comparison-graph')
            ])
        ])
    ], style={'width': '70%', 'float': 'right', 'padding': '10px'})
])

# Callbacks for interactivity
@app.callback(
    Output('current-weather-output', 'children'),
    [Input('current-weather-button', 'n_clicks')],
    [Input('city-dropdown', 'value')]
)
def fetch_current_weather(n_clicks, selected_city):
    if n_clicks > 0:
        # Fetch current weather data
        current_weather_df = dp.get_current_weather(selected_city)

        if current_weather_df is not None and not current_weather_df.empty:
            # Extract weather icon URL
            weather_icon_url = current_weather_df['weather_icons'][0]

            # Create a vertical table (transpose the DataFrame)
            display_df = current_weather_df.drop(columns=['weather_icons']).T.reset_index()
            display_df.columns = ['Attribute', 'Value']  # Rename columns for clarity

            # Create a Plotly table
            table_fig = ff.create_table(
                display_df,
                index=False,
                height_constant=20  # Adjust row height for compactness
            )

            # Update table layout for a narrower table
            table_fig.update_layout(
                autosize=False,
                width=400,  # Set table width
                margin=dict(l=20, r=20, t=10, b=10)  # Reduce margins
            )

            # Render the weather icon above the table
            return html.Div([
                html.Div([
                    html.Img(src=weather_icon_url, style={'width': '50px', 'height': '50px', 'margin-bottom': '10px'}),
                ], style={'text-align': 'center'}),
                dcc.Graph(figure=table_fig, style={'margin-top': '10px'})
            ], style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center'})
        else:
            return "Unable to fetch current weather data. Please try again."
    return "Click the button to fetch current weather."

@app.callback(
    Output('city-info-table', 'children'),
    Input('city-dropdown', 'value')
)
def update_city_info(selected_city):
    # Filter the constants table for the selected city
    city_info = constants[constants['name'] == selected_city].drop(columns=['address'], errors='ignore')

    # Rename columns for clarity
    city_info = city_info.rename(columns={
        'z': 'geopotential (m² s⁻²)',
        'lsm': 'land-sea mask (dimensionless)'
    })

    # Add calculated columns
    city_info['orography (m)'] = city_info['geopotential (m² s⁻²)'] / 9.80665

    # Transpose the DataFrame to make it vertical
    city_info = city_info.T.reset_index()
    city_info.columns = ['Parameter', 'Value']

    # Create the table using plotly's figure factory
    table_figure = ff.create_table(city_info)

    # Add the description for geopotential and land-sea mask
    description = dcc.Markdown('''
    ### Geopotential
    **Units**: m² s⁻²

    This parameter is the gravitational potential energy of a unit mass, at a particular location at the surface of the Earth, relative to mean sea level. It is also the amount of work that would have to be done, against the force of gravity, to lift a unit mass to that location from mean sea level. The (surface) geopotential height (orography) can be calculated by dividing the (surface) geopotential by the Earth's gravitational acceleration, g (=9.80665 m s⁻²). This parameter does not vary in time.

    ### Land-sea mask
    **Units**: Dimensionless

    This parameter is the proportion of land, as opposed to ocean or inland waters (lakes, reservoirs, rivers, and coastal waters), in a grid box. Values range between 0 and 1 and are dimensionless. 
    - A value above 0.5 typically indicates a mix of land and inland water, but not ocean.
    - A value of 0.5 or below indicates primarily a water surface.
    This parameter does not vary in time.
    ''')

    # Return the table and the description
    return html.Div([
        dcc.Graph(figure=table_figure),
        description
    ])

@app.callback(
    [Output('basic-weather-table', 'children'),
     Output('basic-weather-graph', 'figure')],
    [Input('city-dropdown', 'value'),
     Input('start-date', 'date'),
     Input('end-date', 'date'),
     Input('variable-dropdown', 'value')]
)
def update_basic_weather(selected_city, start_date, end_date, selected_variable):
    # Filter daily_data for the selected city and date range
    filtered_data = daily_data[
        (daily_data['name'] == selected_city) &
        (daily_data['date'] >= start_date) &
        (daily_data['date'] <= end_date)
    ].drop(columns=['name'], errors='ignore')

    # Format the 'date' column to remove time
    filtered_data['date'] = pd.to_datetime(filtered_data['date']).dt.date

    # Create a styled scrollable DataTable
    table = dash.dash_table.DataTable(
        data=filtered_data.to_dict('records'),
        columns=[{'name': col, 'id': col} for col in filtered_data.columns],
        style_table={'overflowX': 'auto'},  # Enable horizontal scrolling
        style_cell={
            'textAlign': 'center',  # Center align all text
            'minWidth': '100px', 'width': '100px', 'maxWidth': '100px',  # Equal column widths
            'whiteSpace': 'normal',  # Prevent text from being cut off
        },
        style_header={
            'backgroundColor': 'darkblue',  # Blue background for headers
            'color': 'white',  # White text for headers
            'fontWeight': 'bold',  # Bold text for headers
            'textAlign': 'center',  # Center align header text
        },
        page_size=10  # Display 10 rows per page
    )

    # Generate the graph using the Datapipeline class
    graph_figure = dp.plot_basic_weather(daily_data, selected_city, start_date, end_date, selected_variable)

    return table, graph_figure 

@app.callback(
    [Output('temp-pressure-graph', 'figure'),
     Output('wind-heatmap-graph', 'figure')],
    [Input('city-dropdown', 'value'),
     Input('start-date', 'date'),
     Input('end-date', 'date')]
)
def update_hourly_weather(selected_city, start_date, end_date):
    # Generate plots using the Datapipeline class
    hourly_temp = dp.plot_hourly_temperature(hourly_data, selected_city, start_date, end_date)
    wind_heatmap = dp.plot_wind_heatmap(hourly_data, selected_city, start_date, end_date)

    return hourly_temp, wind_heatmap
@app.callback(
    Output('overall-analysis', 'children'),
    Input('city-dropdown', 'value')
)
def update_overall_analysis(selected_city):
    fig1 = dp.impact_of_humidity_on_temperature(daily_data, selected_city)
    fig2 = dp.cloud_cover_vs_solar_radiation(daily_data, selected_city)
    fig3 = dp.seasonal_analysis(daily_data, selected_city)
    fig4 = dp.extreme_weather_analysis(daily_data, selected_city)
    fig5 = dp.geographical_insights(constants, daily_data)

    return html.Div([
        html.Div([dcc.Graph(figure=fig1)], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(figure=fig2)], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(figure=fig3)], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(figure=fig4)], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(figure=fig5)], style={'width': '100%', 'display': 'inline-block'})
    ])

@app.callback(
    [Output('comparison-table', 'children'),
     Output('comparison-graph', 'figure')],
    [Input('city-dropdown', 'value'),
     Input('comparison-city-dropdown', 'value'),
     Input('start-date', 'date'),
     Input('end-date', 'date')]
)
def update_city_comparison(city1, city2, start_date, end_date):
    # Build comparison table and graph using Datapipeline methods
    comparison_table = dp.build_comparison_table(daily_data, city1, city2, start_date, end_date)
    comparison_graph = dp.build_comparison_graph(daily_data, city1, city2, start_date, end_date)

    # Convert the comparison table to a Dash-friendly format
    table_fig = ff.create_table(comparison_table)

    return dcc.Graph(figure=table_fig), comparison_graph

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
