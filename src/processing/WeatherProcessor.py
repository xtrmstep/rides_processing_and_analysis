import pandas as pd
from time import time


class Files:
    city_attributes = "city_attributes"
    humidity = "humidity"
    pressure = "pressure"
    temperature = "temperature"
    weather_description = "weather_description"
    wind_direction = "wind_direction"
    wind_speed = "wind_speed"


class WeatherProcessing:
    """Routines required for weather raw data processing"""

    def __init__(self, raw_data_folder: str, target_city: str):
        self.files = {
            Files.city_attributes: f"{raw_data_folder}/city_attributes.csv",
            Files.humidity: f"{raw_data_folder}/humidity.csv",
            Files.pressure: f"{raw_data_folder}/pressure.csv",
            Files.temperature: f"{raw_data_folder}/temperature.csv",
            Files.weather_description :  f"{raw_data_folder}/weather_description.csv",
            Files.wind_direction: f"{raw_data_folder}/wind_direction.csv",
            Files.wind_speed: f"{raw_data_folder}/wind_speed.csv"
        }
        self.target_city = target_city

    def process(self):
        # loading data
        df_humidity = self._extract_ny_data(Files.humidity, self.target_city)
        df_pressure = self._extract_ny_data(Files.pressure, self.target_city)
        df_temperature = self._extract_ny_data(Files.temperature, self.target_city)
        df_weather_description = self._extract_ny_data(Files.weather_description, self.target_city)
        df_wind_direction = self._extract_ny_data(Files.wind_direction, self.target_city)
        df_wind_speed = self._extract_ny_data(Files.wind_speed, self.target_city)

        # joining data

        df = df_humidity.join(df_pressure, on=["datetime"], lsuffix="_humidity")
        df = df.join(df_temperature, on=["datetime"], lsuffix="_pressure")
        df = df.join(df_weather_description, on=["datetime"], lsuffix="_temperature")
        df = df.join(df_wind_direction, on=["datetime"], lsuffix="_weather_description")
        df = df.join(df_wind_speed, on=["datetime"], lsuffix="_wind_direction")
        index_columns = [col for col in df.columns if col.startswith("index")]
        df = df.drop(index_columns, axis=1)
        df = df.rename(columns={
            "New York_humidity": "humidity",
            "New York_pressure": "pressure",
            "New York_temperature": "temperature",
            "New York_weather_description": "weather_description",
            "New York_wind_direction": "wind_direction",
            "New York": "wind_speed",
        })

        # fill gaps with interpolation

        df = df.interpolate().fillna(method='bfill')

    def _extract_ny_data(self, source_file: str, city):
        df = pd.read_csv(source_file)
        df = df[["datetime", city]].reset_index()
        df = df.set_index(["datetime"])
        return df
