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
    
    def connect_to_postgres(self, pw, user='postgres', 
                            host='localhost', port='5432',
                            create_datapipeline = False):
        dbserver = psycopg.connect(
            user=user, 
            password=pw, 
            host=host, 
            port=port)
        dbserver.autocommit = True
        if create_datapipeline:
                cursor = dbserver.cursor()
                cursor.execute("DROP DATABASE IF EXISTS datapipeline")
                cursor.execute("CREATE DATABASE datapipeline")
        engine = create_engine(f'postgresql+psycopg://{user}:{pw}@{host}:{port}/datapipeline')
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
    

