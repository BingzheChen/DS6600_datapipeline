import cdsapi

dataset = "reanalysis-era5-land"
request = {
    "variable": [
        "2m_temperature",
        "skin_temperature",
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "surface_pressure",
        "total_precipitation"
    ],
    "year": "2024",
    "month": "10",
    "day": [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12",
        "13", "14", "15",
        "16", "17", "18",
        "19", "20", "21",
        "22", "23", "24",
        "25", "26", "27",
        "28", "29", "30",
        "31"
    ],
    "time": ["00:00", "12:00"],
    "data_format": "grib",
    "download_format": "unarchived",
    "area": [39, -79, 38, -78]
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()
