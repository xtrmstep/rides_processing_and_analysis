import glob
import os
from datetime import datetime
import pandas as pd
import asyncio


class RideGroupsReducer:

    def __init__(self, files_rides_by_days: str, files_rides_by_hours: str,
                 files_rides_by_weekdays: str, files_rides_by_weather: str, destination_folder: str):
        self.files_by_days = glob.glob(files_rides_by_days)
        self.files_by_hours = glob.glob(files_rides_by_hours)
        self.files_by_weekdays = glob.glob(files_rides_by_weekdays)
        self.files_by_weather = glob.glob(files_rides_by_weather)
        self.file_reduced_folder = destination_folder

    async def process(self):
        tasks = [
            asyncio.ensure_future(self._reduce_rides_data(self.files_by_days, "by_day", "days", self.file_reduced_folder)),
            asyncio.ensure_future(self._reduce_rides_data(self.files_by_hours, "by_hour", "hours", self.file_reduced_folder)),
            asyncio.ensure_future(self._reduce_rides_data(self.files_by_weekdays, "by_weekday", "weekdays", self.file_reduced_folder)),
            asyncio.ensure_future(self._reduce_rides_data(self.files_by_weather, "weather_description_code", "weather", self.file_reduced_folder))
        ]
        await asyncio.wait(tasks)

        # merge
        files_by_days = glob.glob(f"{self.file_reduced_folder}/days*.csv")
        files_by_hours = glob.glob(f"{self.file_reduced_folder}/hours*.csv")
        files_by_weekdays = glob.glob(f"{self.file_reduced_folder}/weekdays*.csv")
        files_by_weather = glob.glob(f"{self.file_reduced_folder}/weather*.csv")

        tasks = [
            asyncio.ensure_future(self._merge_files(files_by_days, f"{self.file_reduced_folder}/days.csv")),
            asyncio.ensure_future(self._merge_files(files_by_hours, f"{self.file_reduced_folder}/hours.csv")),
            asyncio.ensure_future(self._merge_files(files_by_weekdays, f"{self.file_reduced_folder}/weekdays.csv")),
            asyncio.ensure_future(self._merge_files(files_by_weather, f"{self.file_reduced_folder}/weather.csv"))
        ]
        await asyncio.wait(tasks)

    async def _merge_files(self, files, dest_filename):
        for file in files:
            df = pd.read_csv(file)

            if os.path.exists(dest_filename):
                df.to_csv(dest_filename, mode='a', header=False, index=False)
            else:
                df.to_csv(dest_filename, index=False)

            os.remove(file)

    async def _reduce_rides_data(self, files, tag_column, tag_name: str, file_reduced_folder: str):

        for filename in files:

            dirname, fname = os.path.split(filename)

            df = pd.read_csv(filename)
            tag_value = df[tag_column].iloc[0]

            reduced = dict(
                tag=tag_name,
                tag_value=tag_value,

                rides_count=len(df),

                passenger_count_mean=df["passenger_count"].mean(),
                passenger_count_median=df["passenger_count"].median(),

                trip_distance_sum=df["trip_distance"].sum(),
                trip_distance_mean=df["trip_distance"].mean(),
                trip_distance_median=df["trip_distance"].median(),

                tip_amount_sum=df["tip_amount"].sum(),
                tip_amount_mean=df["tip_amount"].mean(),
                tip_amount_median=df["tip_amount"].median(),

                humidity_mean=df["humidity"].mean(),
                humidity_median=df["humidity"].median(),

                pressure_mean=df["pressure"].mean(),
                pressure_median=df["pressure"].median(),

                wind_speed_mean=df["wind_speed"].mean(),
                wind_speed_median=df["wind_speed"].median()
            )

            reduced_filename = f"{file_reduced_folder}/{fname}"
            df_reduced = pd.DataFrame([reduced])
            df_reduced.to_csv(reduced_filename, index=False)
