import numpy as np
import pandas as pd
import os
import dotenv
import requests
import json
from bs4 import BeautifulSoup
import psycopg
import pymongo
from bson.json_util import dumps, loads
import sqlite3
from sqlalchemy import create_engine
import plotly.express as px
import cfgrib
import xarray as xr

dotenv.load_dotenv()

class DataPipeline:
    def __init__(self):
        self.mypassword = os.getenv('mypassword')
        self.weatherstackkey = os.getenv('weatherstackkey')
        self.POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
        self.MONGO_INITDB_ROOT_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME')
        self.MONGO_INITDB_ROOT_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
        
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

    def get_current_weather(self):
        url = "https://api.weatherstack.com/current"
        zip_codes = [f"229{i:02}" for i in range(1, 11)]  # Generate zip codes 22901 to 22910
        combined_df = pd.DataFrame()  # Initialize an empty DataFrame

        for zip_code in zip_codes:
            querystring = {
                "access_key": self.weatherstackkey,  # API key
                "query": zip_code,  # Current zip code
                "units": "m"  # Metric units
            }
            headers = self.make_headers()  # Assuming self.make_headers() is implemented

            # Sending the GET request
            response = requests.get(url, params=querystring, headers=headers)

            if response.status_code == 200:
                data = response.json()
                if 'error' not in data:  # Check for API-specific errors
                    print(f"Weather data for {zip_code} retrieved successfully!")
                    request_data = data.get('request', {})
                    location_data = data.get('location', {})
                    current_data = data.get('current', {})

                    # Convert each section into a DataFrame
                    request_df = pd.DataFrame([request_data])
                    location_df = pd.DataFrame([location_data])
                    current_df = pd.DataFrame([current_data])

                    # Combine all into a single DataFrame for the current zip code
                    zip_code_df = pd.concat([request_df, location_df, current_df], axis=1)

                    # Append the current zip code's data to the combined DataFrame
                    combined_df = pd.concat([combined_df, zip_code_df], ignore_index=True)
                else:
                    print(f"Error in API response for {zip_code}: {data['error']}")
            else:
                print(f"HTTP Error for {zip_code}: {response.status_code} - {response.reason}")

        return combined_df
    
    def get_dayly_weather(self):
        file_path = "data/charlottesville 2023-03-01 to 2024-11-01.csv"
        data = pd.read_csv(file_path)
        return data
    
    def get_city_data(self):
        url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=KUCGGQQ58KYENK3BTD9GMXND7&taskId=e2280b4cde99ad754f6ffab62180e01a&zip=false'
        data = pd.read_csv(url)
        return data

    def get_grib_data(self):
        # Path to the already downloaded GRIB file
        grib_file_path = "data/single.grib"

        try:
            print("Reading GRIB file...")
            # Read the GRIB file into an xarray dataset using cfgrib
            ds = xr.open_dataset(grib_file_path, engine="cfgrib")
            print("GRIB file read successfully.")

            # Convert the xarray dataset to a pandas DataFrame
            df = ds.to_dataframe().reset_index()
            print("Data successfully converted to pandas DataFrame.")
            return df
        except Exception as e:
            print(f"Error reading GRIB file: {e}")
            return None
        
    def get_zip_data(self, grib_data):
        """
        Filters grib_data to include only rows where (latitude, longitude) = (38.0, -78.5) or (38.0, -78.6).

        Args:
            grib_data (pd.DataFrame): The DataFrame containing data extracted from the GRIB file.

        Returns:
            pd.DataFrame: Filtered DataFrame with only the required latitude and longitude rows.
        """
        # Ensure grib_data is a DataFrame
        if not isinstance(grib_data, pd.DataFrame):
            raise ValueError("grib_data must be a pandas DataFrame.")

        # Define the target latitude and longitude pairs
        target_coords = [(38.0, -78.5), (38.0, -78.6)]

        # Filter the DataFrame based on the conditions
        filtered_data = grib_data[
            (grib_data['latitude'] == 38.0) &
            (grib_data['longitude'].isin([-78.5, -78.6]))
        ]

        # Reset the index of the filtered DataFrame
        filtered_data = filtered_data.reset_index(drop=True)

        return filtered_data

    def dbml_helper(self, data):
        dt = data.dtypes.reset_index().rename({0:'dtype'}, axis=1)
        replace_map = {'object': 'varchar',
                        'int64': 'int',
                        'float64': 'float',
                        'float32': 'float',
                        'datetime64[ns]': 'varchar'}
        dt['dtype'] = dt['dtype'].replace(replace_map)
        return dt.to_string(index=False, header=False)

