import asyncio
import os
import pandas as pd


class RidesChunkyMapper:
    """Mapping rides data by groups"""

    def __init__(self, file_taxi_rides: str, processing_folder: str, weather_dataset: str,
                 processing_chunk_size: int, ):
        self.file_taxi_rides = file_taxi_rides
        self.file_rides_grouped_hours = f"{processing_folder}/hours_{{0}}.csv"
        self.file_rides_grouped_days = f"{processing_folder}/days_{{0}}.csv"
        self.file_rides_grouped_weekdays = f"{processing_folder}/weekdays_{{0}}.csv"
        self.file_rides_grouped_weather = f"{processing_folder}/weather_{{0}}.csv"
        self.weather_dataset = weather_dataset
        self.processing_chunk_size = processing_chunk_size

    async def _store_groups(self, filename_template, grouped_df):
        for group in grouped_df.groups:
            if isinstance(group, float):
                group = int(group)

            tag = group if isinstance(group, int) else group.strftime("%Y%m%d_%H")
            file_group = filename_template.format(tag)
            if os.path.exists(file_group):
                grouped_df.get_group(group).to_csv(file_group, mode='a', header=False, index=False)
            else:
                grouped_df.get_group(group).to_csv(file_group, index=False)

    def _load_weather_dataframe(self) -> pd.DataFrame:
        file_weather = self.weather_dataset
        df_weather = pd.read_csv(file_weather)
        df_weather['datetime'] = pd.to_datetime(df_weather['datetime'], format="%Y-%m-%d %H:%M:%S")
        df_weather = df_weather.reset_index().set_index("datetime")
        df_weather = df_weather.drop("index", axis=1)
        return df_weather

    async def process(self):
        extracting_columns = [
            'pickup_datetime',
            'dropoff_datetime',
            'passenger_count',
            'trip_distance',
            'payment_type',
            'tip_amount']

        df_weather = self._load_weather_dataframe()
        for chunk in pd.read_csv(self.file_taxi_rides, chunksize=self.processing_chunk_size):

            try:
                chunk = self._strip_spaces_in_columns(chunk)

                chunk['pickup_datetime'] = pd.to_datetime(chunk['pickup_datetime'], format="%Y-%m-%d %H:%M:%S")
                chunk = self._drop_not_used_columns(chunk, extracting_columns)
                chunk = self._join_with_weather(chunk, df_weather)

                # mapping

                # run grouping and storing in parallel threads
                tasks = []

                chunk["by_hour"] = chunk['pickup_datetime'].apply(lambda t: t.replace(second=0, microsecond=0, minute=0))
                task = asyncio.ensure_future(self._process_group(chunk, "by_hour", self.file_rides_grouped_hours))
                tasks.append(task)

                chunk["by_day"] = chunk['pickup_datetime'].apply(lambda t: t.date())
                task = asyncio.ensure_future(self._process_group(chunk, "by_day", self.file_rides_grouped_days))
                tasks.append(task)

                chunk["by_weekday"] = chunk['pickup_datetime'].apply(lambda t: t.weekday())
                task = asyncio.ensure_future(self._process_group(chunk, "by_weekday", self.file_rides_grouped_weekdays))
                tasks.append(task)

                task = asyncio.ensure_future(self._process_group(chunk, "weather_description_code",
                                                                 self.file_rides_grouped_weather))
                tasks.append(task)

                await asyncio.wait(tasks)

                break

            except Exception as err:
                print(err)
                raise

    def _join_with_weather(self, df, df_weather):
        df["datetime"] = df['pickup_datetime'].apply(lambda t: t.replace(second=0, microsecond=0, minute=0))
        df = df.reset_index().set_index("datetime")
        df = df.drop("index", axis=1)
        df = df.join(df_weather, on="datetime")
        return df

    def _drop_not_used_columns(self, df, necessary_columns):
        columns_to_drop = [col for col in df.columns if col not in necessary_columns]
        df = df.drop(columns_to_drop, axis=1)
        return df

    def _strip_spaces_in_columns(self, df):
        columns_trimming = {col: col.strip() for col in df.columns}
        df = df.rename(columns=columns_trimming)
        return df

    async def _process_group(self, df, group_key_name: str, destination_filename: str):
        groups = df.groupby(group_key_name)
        await self._store_groups(destination_filename, groups)
