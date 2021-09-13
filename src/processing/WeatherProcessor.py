import pandas as pd


class Files:
    city_attributes = "city_attributes"
    humidity = "humidity"
    pressure = "pressure"
    temperature = "temperature"
    weather_description = "weather_description"
    wind_direction = "wind_direction"
    wind_speed = "wind_speed"


class WeatherProcessor:
    """Routines required for weather raw data processing"""

    def __init__(self, raw_data_folder: str, target_city: str, destination_filename: str):
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
        self.destination_filename = destination_filename

    def process(self):
        # loading data
        df_humidity = self._extract_ny_data(self.files[Files.humidity], self.target_city)
        df_pressure = self._extract_ny_data(self.files[Files.pressure], self.target_city)
        df_temperature = self._extract_ny_data(self.files[Files.temperature], self.target_city)
        df_weather_description = self._extract_ny_data(self.files[Files.weather_description], self.target_city)
        df_wind_direction = self._extract_ny_data(self.files[Files.wind_direction], self.target_city)
        df_wind_speed = self._extract_ny_data(self.files[Files.wind_speed], self.target_city)

        # joining data

        df = df_humidity.join(df_pressure, on=["datetime"], lsuffix="_humidity")
        df = df.join(df_temperature, on=["datetime"], lsuffix="_pressure")
        df = df.join(df_weather_description, on=["datetime"], lsuffix="_temperature")
        df = df.join(df_wind_direction, on=["datetime"], lsuffix="_weather_description")
        df = df.join(df_wind_speed, on=["datetime"], lsuffix="_wind_direction")
        index_columns = [col for col in df.columns if col.startswith("index")]
        df = df.drop(index_columns, axis=1)
        df = df.rename(columns={
            f"{self.target_city}_humidity": "humidity",
            f"{self.target_city}_pressure": "pressure",
            f"{self.target_city}_temperature": "temperature",
            f"{self.target_city}_weather_description": "weather_description",
            f"{self.target_city}_wind_direction": "wind_direction",
            self.target_city: "wind_speed",
        })

        # add numeric code for weather type
        df["weather_description_code"] = df[["weather_description"]].applymap(self._map_weather_to_number)

        # fill gaps with interpolation

        df = df.interpolate().fillna(method='bfill')

        df.reset_index().to_csv(self.destination_filename, index=False)

    def _extract_ny_data(self, source_file: str, city):
        df = pd.read_csv(source_file)
        df = df[["datetime", city]].reset_index()
        df = df.set_index(["datetime"])
        return df

    def _map_weather_to_number(self, weather):
        mapping = {
            'few clouds': 1,
            'sky is clear': 2,
            'scattered clouds': 3,
            'broken clouds': 4,
            'overcast clouds': 5,
            'mist': 6,
            'drizzle': 7,
            'moderate rain': 8,
            'light intensity drizzle': 9,
            'light rain': 10,
            'fog': 11,
            'haze': 12,
            'heavy snow': 13,
            'heavy intensity drizzle': 14,
            'heavy intensity rain': 15,
            'light rain and snow': 16,
            'snow': 17,
            'light snow': 18,
            'freezing rain': 19,
            'proximity thunderstorm': 20,
            'thunderstorm': 21,
            'thunderstorm with rain': 22,
            'smoke': 23,
            'very heavy rain': 24,
            'thunderstorm with heavy rain': 25,
            'thunderstorm with light rain': 26,
            'squalls': 27,
            'dust': 28,
            'proximity thunderstorm with rain': 29,
            'thunderstorm with light drizzle': 30,
            'sand': 31,
            'shower rain': 32,
            'proximity thunderstorm with drizzle': 33,
            'light intensity shower rain': 34,
            'sand/dust whirls': 35,
            'heavy thunderstorm': 36
        }
        return mapping[weather] if weather in mapping else None
