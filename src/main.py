import processing
import asyncio
from time import time


async def main():

    start = time()
    
    folder_data = "ABSOLUTE_PATH/data/"
    # folder_data = "./data/"
    folder_for_processing_files = f"{folder_data}processing/"
    folder_for_reduced_files = f"{folder_data}reduced/"
    file_weather_dataset = f"{folder_data}prepared_weather.csv"

    processing.WeatherProcessor(raw_data_folder=f"{folder_data}weather/",
                                target_city="New York",
                                destination_filename=file_weather_dataset)\
        .process()
    await processing.RidesChunkyMapper(file_taxi_rides=f"{folder_data}yellow_tripdata_2014-01/yellow_tripdata_2014-01.csv",
                                       processing_folder=f"{folder_for_processing_files}",
                                       weather_dataset=file_weather_dataset,
                                       processing_chunk_size=1000000 * 3)\
        .process()

    await processing.RideGroupsReducer(files_rides_by_days=f"{folder_for_processing_files}/days*.csv",
                                       files_rides_by_hours=f"{folder_for_processing_files}/hours*.csv",
                                       files_rides_by_weekdays=f"{folder_for_processing_files}/weekdays*.csv",
                                       files_rides_by_weather=f"{folder_for_processing_files}/weather*.csv",
                                       destination_folder=folder_for_reduced_files)\
        .process()

    print(f"Done in {time()-start} sec")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
