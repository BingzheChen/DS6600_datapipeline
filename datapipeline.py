import numpy as np
import pandas as pd
import os
import dotenv
import requests
import json
# import psycopg
from sqlalchemy import create_engine
import plotly.express as px
import xarray as xr
import plotly.graph_objs as go
import pymysql

dotenv.load_dotenv()

class DataPipeline:
    def __init__(self):
        self.mypassword = os.getenv('mypassword')
        self.weatherstackkey = os.getenv('weatherstackkey')
        self.POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
        self.MYSQL_PASSWORD = os.getenv('MYSQL_ROOT_PASSWORD')
        self.MONGO_INITDB_ROOT_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME')
        self.MONGO_INITDB_ROOT_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
        self.target_coords = [
            {"name": "lasvegas", "latitude": 36.25, "longitude": -115.00},
            {"name": "chicago", "latitude": 42.00, "longitude": -87.75},
            {"name": "seattle", "latitude": 47.50, "longitude": -122.25},
            {"name": "houston", "latitude": 29.75, "longitude": -95.25},
            {"name": "denver", "latitude": 39.75, "longitude": -105.00},
            {"name": "losangeles", "latitude": 34.00, "longitude": -118.25},
            {"name": "miami", "latitude": 25.75, "longitude": -80.25},
            {"name": "charlottesville", "latitude": 38.00, "longitude": -78.50},
            {"name": "boston", "latitude": 42.50, "longitude": -71.00}
        ]
        
    def get_useragent(self):
        url = 'https://httpbin.org/user-agent'
        r = requests.get(url)
        useragent = json.loads(r.text)['user-agent']
        return useragent
    
    def make_headers(self,  
                     email='fvq3xv@virginia.edu'):
            useragent=self.get_useragent()
            headers = {
                'User-Agent': useragent,
                'From': email
            }
            return headers

    def get_current_weather(self, city_name):
        """
        Fetches current weather data for a given city name and returns selected columns.

        Args:
            city_name (str): Name of the city for which to fetch the current weather.

        Returns:
            pd.DataFrame: A DataFrame containing selected current weather data for the specified city.
        """
        url = "https://api.weatherstack.com/current"

        # Handle specific cases for city names
        query_city = city_name
        if city_name.lower() == "lasvegas":
            query_city = "las vegas"
        elif city_name.lower() == "losangeles":
            query_city = "los angeles"

        # Define the querystring for the API request
        querystring = {
            "access_key": self.weatherstackkey,  # API key
            "query": query_city,  # Adjusted city name
            "units": "m"  # Metric units
        }
        headers = self.make_headers()  # Assuming self.make_headers() is implemented

        # Sending the GET request
        response = requests.get(url, params=querystring, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if 'error' not in data:  # Check for API-specific errors
                # print(f"Weather data for {query_city} retrieved successfully!")

                # Extract required fields
                selected_data = {
                    "localtime": data.get("location", {}).get("localtime"),
                    "temperature": data.get("current", {}).get("temperature"),
                    "weather_icons": data.get("current", {}).get("weather_icons", [None])[0],  # Get the first icon URL
                    "wind_degree": data.get("current", {}).get("wind_degree"),
                    "wind_dir": data.get("current", {}).get("wind_dir"),
                    "precip": data.get("current", {}).get("precip"),
                    "humidity": data.get("current", {}).get("humidity"),
                    "feelslike": data.get("current", {}).get("feelslike"),
                    "visibility": data.get("current", {}).get("visibility"),
                }

                # Convert the dictionary into a DataFrame
                return pd.DataFrame([selected_data])

            else:
                print(f"Error in API response for {query_city}: {data['error']}")
                return None
        else:
            print(f"HTTP Error for {query_city}: {response.status_code} - {response.reason}")
            return None
    
    def get_dayly_weather(self):
        file_path = "data/charlottesville 2023-03-01 to 2024-11-01.csv"
        data = pd.read_csv(file_path)
        return data
    
    def get_city_data(self):
        urls = [
            'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=KUCGGQQ58KYENK3BTD9GMXND7&taskId=f0995135769f9d08b3f696817b0c41e2&zip=false',  # URL 4
            'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=KUCGGQQ58KYENK3BTD9GMXND7&taskId=0f28c2478c80e2039b41b261d888984a&zip=false',  # URL 3
            'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=KUCGGQQ58KYENK3BTD9GMXND7&taskId=59258debcdd5b0403d2b24bdc5eb37cf&zip=false',  # URL 2
            'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=KUCGGQQ58KYENK3BTD9GMXND7&taskId=e2280b4cde99ad754f6ffab62180e01a&zip=false'   # URL 1
        ]

        # Initialize an empty DataFrame to store combined data
        combined_data = pd.DataFrame()

        # Read and append data from each URL
        for url in urls:
            try:
                print(f"Reading data from {url}...")
                data = pd.read_csv(url)
                combined_data = pd.concat([combined_data, data], ignore_index=True)
            except Exception as e:
                print(f"Error reading data from {url}: {e}")
                continue
            
        combined_data = combined_data[combined_data['name'].str.lower() != "newyork"]

        return combined_data

    def get_city_info(self):
        data = self.get_city_data()
        # Select relevant columns
        columns_to_select = ['name', 'address', 'resolvedAddress', 'latitude', 'longitude']
        # Filter for unique 'name' values
        unique_data = data[columns_to_select].drop_duplicates(subset='name').reset_index(drop=True)
        return(unique_data)

    def get_grib_data(self):
        """
        Reads two local GRIB files and combines their contents into a single pandas DataFrame.

        Returns:
            pd.DataFrame: Combined data from the two GRIB files as a DataFrame.
        """
        # Paths to the locally downloaded GRIB files
        grib_file_paths = [
            "data/6d689956151f055eb4faef10a270a18f.grib",
            "data/522c7966bf7121a0ef80a12ec223d5af.grib"
        ]

        combined_df = pd.DataFrame()  # Initialize an empty DataFrame to store combined data

        for grib_file_path in grib_file_paths:
            try:
                print(f"Reading GRIB data from file: {grib_file_path}")

                # Read the GRIB file into an xarray Dataset
                ds = xr.open_dataset(grib_file_path, engine="cfgrib")

                # Convert the xarray Dataset to a pandas DataFrame
                print(f"Converting GRIB data from {grib_file_path} to DataFrame...")
                df = ds.to_dataframe().reset_index()

                # Append the DataFrame to the combined DataFrame
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                print(f"Data from {grib_file_path} added successfully.")

            except Exception as e:
                print(f"Error processing GRIB file {grib_file_path}: {e}")

        print("All GRIB files processed successfully.")
        return combined_df
        
    def get_zip_data(self, grib_data):
        """
        Filters grib_data to include only rows where (latitude, longitude) match predefined locations (rounded to one decimal place).
        Adds a 'name' column corresponding to the location name and removes 'latitude' and 'longitude' columns.

        Args:
            grib_data (pd.DataFrame): The DataFrame containing data extracted from the GRIB file.

        Returns:
            pd.DataFrame: Filtered DataFrame with location names and no latitude/longitude columns.
        """
        # Ensure grib_data is a DataFrame
        if not isinstance(grib_data, pd.DataFrame):
            raise ValueError("grib_data must be a pandas DataFrame.")

        # Build a DataFrame for the target coordinates
        target_df = pd.DataFrame(self.target_coords)

        # Merge grib_data with target_df on latitude and longitude
        filtered_data = grib_data.merge(
            target_df,
            how="inner",
            on=["latitude", "longitude"]
        )

        # Remove latitude and longitude columns
        filtered_data = filtered_data.drop(columns=["latitude", "longitude"])

        # Reset the index of the filtered DataFrame
        filtered_data = filtered_data.reset_index(drop=True)

        return filtered_data
    
    def make_constants_tabel(self, zip_data):
        """
        Extracts 'z' and 'lsm' values for 9 cities from zip_data and merges them with city_info.

        Args:
            zip_data (pd.DataFrame): DataFrame containing weather data, including 'z' and 'lsm' columns.

        Returns:
            pd.DataFrame: A DataFrame containing city information enriched with 'z' and 'lsm' values.
        """
        # Get city information DataFrame
        city_info = self.get_city_info()  # Assuming this returns a DataFrame with city names and other information

        # Extract the first occurrence of 'z' and 'lsm' for each city
        constants = (
            zip_data.groupby("name")[["z", "lsm"]]
            .first()  # Take the first occurrence of 'z' and 'lsm' for each city
            .reset_index()  # Reset index to make 'name' a column
        )

        # Merge 'constants' DataFrame with 'city_info' based on 'name'
        enriched_city_info = pd.merge(city_info, constants, on="name", how="left")

        return enriched_city_info
    
    def hourly_data(self, zip_data):
        """
        Processes zip_data by removing specified columns, sorting, and splitting time into date and time columns.

        Args:
            zip_data (pd.DataFrame): DataFrame containing weather data with various columns.

        Returns:
            pd.DataFrame: Processed DataFrame with specified modifications.
        """
        # Drop unnecessary columns
        columns_to_drop = ["number", "step", "valid_time", "z", "lsm"]
        processed_data = zip_data.drop(columns=columns_to_drop, errors="ignore")

        # Sort the data by 'name' and 'time'
        processed_data = processed_data.sort_values(by=["name", "time"]).reset_index(drop=True)
        
        # Ensure the 'time' column is a string
        processed_data["time"] = processed_data["time"].astype(str)

        # Split 'time' into 'date' and 'time' columns
        processed_data["date"] = processed_data["time"].str.split(" ").str[0]
        processed_data["time"] = processed_data["time"].str.split(" ").str[1]

        # Convert 't2m' and 'skt' from Kelvin to Celsius
        if "t2m" in processed_data.columns:
            processed_data["t2m"] = processed_data["t2m"] - 273.15
        if "skt" in processed_data.columns:
            processed_data["skt"] = processed_data["skt"] - 273.15
            
        # Rearrange columns so 'date' and 'time' come after 'name'
        column_order = ["name", "date", "time"] + [col for col in processed_data.columns if col not in ["name", "date", "time"]]
        processed_data = processed_data[column_order]

        return processed_data
    
    def daily_data(self, city_data):
        """
        Processes city_data by parsing datetime, sorting, resetting the index, 
        and cleaning up unnecessary columns.

        Args:
            city_data (pd.DataFrame): DataFrame containing city weather data.

        Returns:
            pd.DataFrame: Processed DataFrame with specified modifications.
        """
        # Ensure 'datetime' is properly parsed as a datetime object
        city_data['datetime'] = pd.to_datetime(city_data['datetime'])

        # Sort the DataFrame by 'name' and 'datetime'
        city_data = city_data.sort_values(by=['name', 'datetime'], ascending=[True, True])

        # Reset the index after sorting
        city_data = city_data.reset_index(drop=True)

        # Delete unnecessary columns
        columns_to_drop = ['address', 'resolvedAddress', 'latitude', 'longitude', 'icon']
        city_data = city_data.drop(columns=columns_to_drop, errors='ignore')

        # Rename 'datetime' to 'date'
        city_data = city_data.rename(columns={'datetime': 'date'})

        return city_data


    def dbml_helper(self, data):
        dt = data.dtypes.reset_index().rename({0:'dtype'}, axis=1)
        replace_map = {'object': 'varchar',
                        'int64': 'int',
                        'float64': 'float',
                        'float32': 'float',
                        'datetime64[ns]': 'varchar'}
        dt['dtype'] = dt['dtype'].replace(replace_map)
        return dt.to_string(index=False, header=False)
    
    # def connect_to_postgres(self, pw, user='postgres', 
    #                         host='localhost', port='5432',
    #                         create_datapipeline = False):
    #     dbserver = psycopg.connect(
    #         user=user, 
    #         password=pw, 
    #         host=host, 
    #         port=port)
    #     dbserver.autocommit = True
    #     if create_datapipeline:
    #             cursor = dbserver.cursor()
    #             cursor.execute("DROP DATABASE IF EXISTS datapipeline")
    #             cursor.execute("CREATE DATABASE datapipeline")
    #     engine = create_engine(f'postgresql+psycopg://{user}:{pw}@{host}:{port}/datapipeline')
    #     return dbserver, engine
    
    def connect_to_mysql(self, pw, user='root', host='localhost', port='3306', create_datapipeline=False):
        """
        Connect to MySQL and optionally create the `datapipeline` database.
        
        Args:
            pw (str): MySQL root password.
            user (str): MySQL username.
            host (str): MySQL host address.
            port (str): MySQL port number.
            create_datapipeline (bool): Whether to drop and create the `datapipeline` database.

        Returns:
            dbserver: pymysql connection object.
            engine: SQLAlchemy engine for MySQL.
        """
        # Connect to MySQL server
        dbserver = pymysql.connect(
            user=user,
            password=pw,
            host=host,
            port=int(port),
            database=None  # Connect to MySQL without specifying a database
        )

        # Autocommit to allow database creation
        dbserver.autocommit(True)

        if create_datapipeline:
            cursor = dbserver.cursor()
            cursor.execute("DROP DATABASE IF EXISTS datapipeline")
            cursor.execute("CREATE DATABASE datapipeline")
            print("Database `datapipeline` created successfully in MySQL.")

        # Create SQLAlchemy engine for the newly created database
        engine = create_engine(f'mysql+pymysql://{user}:{pw}@{host}:{port}/datapipeline')
        return dbserver, engine   
    
    def make_dailydata_df(self, daily_data, engine):
        daily_data.columns = daily_data.columns.str.lower()
        daily_data.to_sql('dailydata', con=engine, index=False, chunksize=1000, if_exists='replace')
    
    def make_hourlydata_df(self, hourly_data, engine):
        hourly_data.columns = hourly_data.columns.str.lower()
        hourly_data.to_sql('hourlydata', con=engine, index=False, chunksize=1000, if_exists='replace')
    
    def make_constants_df(self, constants, engine):
        constants.columns = constants.columns.str.lower()
        constants.to_sql('constants', con=engine, index=False, chunksize=1000, if_exists='replace')
    
    ### Analysis ###

    def plot_basic_weather(self, daily_data, selected_city, start_date, end_date, selected_variable):
        """
        Generates a line plot for the selected variable or group of variables in the specified date range and city.

        Args:
            daily_data (pd.DataFrame): The DataFrame containing daily weather data.
            selected_city (str): The name of the city.
            start_date (str): The starting date in 'YYYY-MM-DD' format.
            end_date (str): The ending date in 'YYYY-MM-DD' format.
            selected_variable (str): The weather variable or group to plot.

        Returns:
            go.Figure: A Plotly figure object for the graph.
        """
        # Filter data for the selected city and date range
        filtered_data = daily_data[
            (daily_data['name'] == selected_city) &
            (daily_data['date'] >= start_date) &
            (daily_data['date'] <= end_date)
        ]

        # Ensure the date column is properly formatted
        filtered_data['date'] = pd.to_datetime(filtered_data['date'])

        # Define variable groups
        variable_groups = {
            'temperature': ['tempmax', 'tempmin', 'temp', 'feelslikemax', 'feelslikemin', 'feelslike'],
            'windspeed': ['windspeedmax', 'windspeedmin', 'windspeedmean'],
            'precipitation': ['precip', 'precipprob']
        }

        # Determine the variables to plot
        if selected_variable in variable_groups:
            variables_to_plot = variable_groups[selected_variable]
        else:
            variables_to_plot = [selected_variable]

        # Create the line plot
        fig = go.Figure()

        for variable in variables_to_plot:
            if variable in filtered_data.columns:
                fig.add_trace(go.Scatter(
                    x=filtered_data['date'],  # X-axis: Dates
                    y=filtered_data[variable],  # Y-axis: Variable data
                    mode='lines+markers',  # Line plot with markers
                    name=variable.capitalize(),  # Legend entry
                    line=dict(width=2),  # Customize line style
                ))

        # Customize layout
        fig.update_layout(
            title=f"{selected_variable.capitalize()} Trends in {selected_city} ({start_date} to {end_date})",
            xaxis_title="Date",
            yaxis_title="Value",
            xaxis=dict(showgrid=True, gridcolor='lightgray', tickformat='%Y-%m-%d'),
            yaxis=dict(showgrid=True, gridcolor='lightgray'),
            plot_bgcolor='white',
            font=dict(size=12),
            legend=dict(title="Variables", orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
        )

        return fig

    def plot_wind_heatmap(self, hourly_data, selected_city, start_date, end_date):
        """
        Generates a heatmap for hourly wind speed patterns.

        Args:
            hourly_data (pd.DataFrame): DataFrame containing hourly weather data.
            selected_city (str): Name of the city.
            start_date (str): Start date (YYYY-MM-DD).
            end_date (str): End date (YYYY-MM-DD).

        Returns:
            go.Figure: A Plotly figure object for wind speed heatmap.
        """
        # Filter data for the selected city and date range
        filtered_data = hourly_data[
            (hourly_data['name'] == selected_city) &
            (hourly_data['date'] >= start_date) &
            (hourly_data['date'] <= end_date)
        ]

        if filtered_data.empty:
            print("No data available for the selected city and date range.")
            return go.Figure()

        # Calculate wind speed from u10 and v10
        if 'u10' in filtered_data.columns and 'v10' in filtered_data.columns:
            filtered_data['wind_speed'] = np.sqrt(filtered_data['u10'] ** 2 + filtered_data['v10'] ** 2)
        else:
            print("No wind data available.")
            return go.Figure()

        # Create the wind speed heatmap
        fig = go.Figure(data=go.Heatmap(
            x=filtered_data['time'],  # Use 'time' directly
            y=filtered_data['date'],
            z=filtered_data['wind_speed'],
            colorscale='Viridis',
            colorbar=dict(title="Wind Speed (m/s)")
        ))

        fig.update_layout(
            title=f"Hourly Wind Speed Heatmap in {selected_city}",
            xaxis_title="Time",
            yaxis_title="Date",
            font=dict(size=12),
            plot_bgcolor='white'
        )

        return fig
    
    def plot_hourly_temperature(self, hourly_data, selected_city, start_date, end_date):
        """
        Generates a plot for diurnal temperature patterns.

        Args:
            hourly_data (pd.DataFrame): DataFrame containing hourly weather data.
            selected_city (str): Name of the city.
            start_date (str): Start date (YYYY-MM-DD).
            end_date (str): End date (YYYY-MM-DD).

        Returns:
            go.Figure: A Plotly figure object for hourly temperature.
        """
        # Filter data for the selected city and date range
        filtered_data = hourly_data[
            (hourly_data['name'] == selected_city) &
            (hourly_data['date'] >= start_date) &
            (hourly_data['date'] <= end_date)
        ]

        hourly_averages = filtered_data.groupby('time')['t2m'].mean()

        # Create the temperature plot
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=hourly_averages.index,
            y=hourly_averages,  
            mode='lines+markers',
            name='Temperature (°C)',
            line=dict(color='red', width=2)
        ))

        fig.update_layout(
            title=f"Diurnal Temperature Patterns in {selected_city}",
            xaxis_title="Hour of Day",
            yaxis_title="Temperature (°C)",
            xaxis=dict(tickmode='linear', dtick=1),
            plot_bgcolor='white',
            font=dict(size=12)
        )

        return fig
    
    def impact_of_humidity_on_temperature(self, daily_data, selected_city):
        """
        Explore the relationship between humidity and temperature for the selected city.

        Args:
            daily_data (pd.DataFrame): DataFrame containing daily weather data.
            selected_city (str): Name of the city.

        Returns:
            go.Figure: Scatter plot of humidity vs. temperature with regression line.
        """
        city_data = daily_data[daily_data['name'] == selected_city]

        fig = go.Figure()

        # Scatter plot
        fig.add_trace(go.Scatter(
            x=city_data['humidity'],
            y=city_data['temp'],
            mode='markers',
            name='Humidity vs Temp',
            marker=dict(color='blue', size=8, opacity=0.6)
        ))

        # Fit regression line
        if not city_data.empty:
            slope, intercept = np.polyfit(city_data['humidity'], city_data['temp'], 1)
            regression_line = slope * city_data['humidity'] + intercept
            fig.add_trace(go.Scatter(
                x=city_data['humidity'],
                y=regression_line,
                mode='lines',
                name='Regression Line',
                line=dict(color='red', dash='dash')
            ))

        fig.update_layout(
            title=f"Impact of Humidity on Temperature in {selected_city}",
            xaxis_title="Humidity (%)",
            yaxis_title="Temperature (°C)",
            plot_bgcolor='white',
            font=dict(size=12)
        )

        return fig

    def cloud_cover_vs_solar_radiation(self, daily_data, selected_city):
        """
        Scatter plot of cloud cover vs. solar radiation for the selected city with a regression line.

        Args:
            daily_data (pd.DataFrame): DataFrame containing daily weather data.
            selected_city (str): Name of the selected city.

        Returns:
            go.Figure: Scatter plot for cloud cover vs solar radiation with regression line.
        """
        # Filter data for the selected city
        city_data = daily_data[daily_data['name'] == selected_city]

        fig = go.Figure()

        # Scatter plot
        fig.add_trace(go.Scatter(
            x=city_data['cloudcover'],
            y=city_data['solarradiation'],
            mode='markers',
            name=f"{selected_city}",
            marker=dict(size=8, opacity=0.6)
        ))

        # Add regression line
        if not city_data.empty:
            slope, intercept = np.polyfit(city_data['cloudcover'], city_data['solarradiation'], 1)
            regression_line = slope * city_data['cloudcover'] + intercept
            fig.add_trace(go.Scatter(
                x=city_data['cloudcover'],
                y=regression_line,
                mode='lines',
                name='Regression Line',
                line=dict(color='red', dash='dash')
            ))

        # Update layout
        fig.update_layout(
            title=f"Cloud Cover vs Solar Radiation in {selected_city}",
            xaxis_title="Cloud Cover (%)",
            yaxis_title="Solar Radiation (W/m²)",
            plot_bgcolor='white',
            font=dict(size=12)
        )

        return fig

    def seasonal_analysis(self, daily_data, selected_city):
        """
        Bar chart showing seasonal averages for temperature, precipitation, and humidity for the selected city.

        Args:
            daily_data (pd.DataFrame): DataFrame containing daily weather data.
            selected_city (str): Name of the selected city.

        Returns:
            go.Figure: Bar chart for seasonal analysis.
        """
        # Define a mapping for season numbers to names
        season_mapping = {1: 'Winter', 2: 'Spring', 3: 'Summer', 4: 'Fall'}

        # Filter data for the selected city
        city_data = daily_data[daily_data['name'] == selected_city]
        city_data['season'] = pd.to_datetime(city_data['date']).dt.month % 12 // 3 + 1

        # Replace season numbers with season names
        city_data['season'] = city_data['season'].map(season_mapping)

        # Calculate seasonal averages
        seasonal_averages = city_data.groupby('season')[['temp', 'precip', 'humidity']].mean().reset_index()

        # Create bar chart
        fig = go.Figure()

        for variable in ['temp', 'precip', 'humidity']:
            fig.add_trace(go.Bar(
                x=seasonal_averages['season'],
                y=seasonal_averages[variable],
                name=f"{variable.capitalize()}",
            ))

        # Update layout
        fig.update_layout(
            title=f"Seasonal Weather Patterns in {selected_city}",
            xaxis_title="Season",
            yaxis_title="Value",
            barmode='group',
            plot_bgcolor='white',
            font=dict(size=12)
        )

        return fig

    def extreme_weather_analysis(self, daily_data, selected_city):
        """
        Heatmap showing the frequency of extreme weather events for the selected city.

        Args:
            daily_data (pd.DataFrame): DataFrame containing daily weather data.
            selected_city (str): Name of the selected city.

        Returns:
            go.Figure: Heatmap for extreme weather events.
        """
        city_data = daily_data[daily_data['name'] == selected_city]
        city_data['extreme_temp'] = (city_data['tempmax'] > 35) | (city_data['tempmin'] < -10)
        city_data['extreme_precip'] = city_data['precip'] > 50

        extreme_events = city_data.groupby('date')[['extreme_temp', 'extreme_precip']].sum().reset_index()

        fig = go.Figure(data=go.Heatmap(
            x=extreme_events['date'],
            y=['Extreme Weather Events'] * len(extreme_events),
            z=extreme_events['extreme_temp'] + extreme_events['extreme_precip'],
            colorscale='Reds',
            colorbar=dict(title="Extreme Events")
        ))

        fig.update_layout(
            title=f"Extreme Weather Events in {selected_city}",
            xaxis_title="Date",
            yaxis_title="Frequency",
            plot_bgcolor='white'
        )

        return fig

    def geographical_insights(self, constants, daily_data):
        """
        Scatter plot matrix showing relationships between geographical and weather variables across all cities.

        Args:
            constants (pd.DataFrame): DataFrame containing geographical data.
            daily_data (pd.DataFrame): DataFrame containing daily weather data.

        Returns:
            go.Figure: Scatter plot matrix for geographical insights.
        """
        # Select only numeric columns for mean calculation
        numeric_columns = ['temp', 'precip', 'windspeedmean']
        aggregated_data = daily_data.groupby('name')[numeric_columns].mean().reset_index()

        # Merge geographical and weather data for all cities
        combined_data = constants.merge(aggregated_data, on='name', how='inner')

        # Select relevant variables for correlation analysis
        variables = ['latitude', 'longitude', 'z', 'lsm', 'temp', 'windspeedmean', 'precip']

        # Create scatter plot matrix
        scatter_matrix = px.scatter_matrix(
            combined_data,
            dimensions=variables,
            title="Geographical Insights Across Cities",
            labels={
                "latitude": "Latitude",
                "longitude": "Longitude",
                "z": "Elevation (m²/s²)",
                "lsm": "Land-Sea Mask",
                "temp": "Temperature (°C)",
                "windspeedmean": "Wind Speed (m/s)",
                "precip": "Precipitation (mm)"
            }
        )

        # Adjust figure size to make it square and improve readability
        scatter_matrix.update_layout(
            plot_bgcolor='white',
            font=dict(size=12),
            autosize=False,
            height=800,  # Square-shaped
            width=800,   # Square-shaped
            margin=dict(l=50, r=50, t=50, b=50)
        )

        return scatter_matrix

    def build_comparison_table(self, daily_data, city1, city2, start_date, end_date):
        """
        Build a detailed table comparing weather data for two cities over a specific date range.

        Args:
            daily_data (pd.DataFrame): DataFrame containing daily weather data.
            city1 (str): Name of the first city.
            city2 (str): Name of the second city.
            start_date (str): Start date (YYYY-MM-DD).
            end_date (str): End date (YYYY-MM-DD).

        Returns:
            pd.DataFrame: Detailed comparison table.
        """
        # Filter data for the selected cities and date range
        filtered_data = daily_data[
            (daily_data['name'].isin([city1, city2])) &
            (daily_data['date'] >= start_date) &
            (daily_data['date'] <= end_date)
        ]

        # Initialize an empty list to store city-specific data
        rows = []

        for city in [city1, city2]:
            city_data = filtered_data[filtered_data['name'] == city]
            metrics = {
                'City': city,
                'Avg Temp (°C)': round(city_data['temp'].mean(), 2),
                'Std Temp (°C)': round(city_data['temp'].std(), 2),
                'Min Temp (°C)': round(city_data['tempmin'].min(), 2),
                'Max Temp (°C)': round(city_data['tempmax'].max(), 2),
                'Avg Wind Speed (m/s)': round(city_data['windspeedmean'].mean(), 2),
                'Avg Precip (mm)': round(city_data['precip'].mean(), 2),
                'Std Precip (mm)': round(city_data['precip'].std(), 2),
                'Max Precip (mm)': round(city_data['precip'].max(), 2),
                'Avg Humidity (%)': round(city_data['humidity'].mean(), 2),
            }
            rows.append(metrics)

        # Convert the list of dictionaries to a DataFrame
        comparison_data = pd.DataFrame(rows)

        return comparison_data

    def build_comparison_graph(self, daily_data, city1, city2, start_date, end_date):
        """
        Build a graph comparing weather trends for two cities over a specific date range.

        Args:
            daily_data (pd.DataFrame): DataFrame containing daily weather data.
            city1 (str): Name of the first city.
            city2 (str): Name of the second city.
            start_date (str): Start date (YYYY-MM-DD).
            end_date (str): End date (YYYY-MM-DD).

        Returns:
            go.Figure: A Plotly figure comparing weather trends.
        """
        # Filter data for the selected cities and date range
        filtered_data = daily_data[
            (daily_data['name'].isin([city1, city2])) &
            (daily_data['date'] >= start_date) &
            (daily_data['date'] <= end_date)
        ]

        # Create line plot for temperature trends
        fig = go.Figure()

        for city in [city1, city2]:
            city_data = filtered_data[filtered_data['name'] == city]
            fig.add_trace(go.Scatter(
                x=city_data['date'],
                y=city_data['temp'],
                mode='lines+markers',
                name=f"{city} - Temperature",
                line=dict(width=2)
            ))

        fig.update_layout(
            title=f"Weather Trends Comparison: {city1} vs {city2}",
            xaxis_title="Date",
            yaxis_title="Temperature (°C)",
            plot_bgcolor='white',
            font=dict(size=12)
        )

        return fig




