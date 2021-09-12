# Analyzing the effect of weather on ridership

Types of dependencies:

- Number and distance of rides by weather conditions
- Number and distance of rides per day, hour, weekday

## Datasets

- [Taxi rides dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page), 2014, January, Yellow Taxi Trip Records
- [Weather dataset](https://www.kaggle.com/selfishgene/historical-hourly-weather-data), hourly data from 2012 to 2017

## Processing

Before processing make sure that you have following files at locations:

- folder `./data` exists
- empty folder `./data/processing`
- empty folder `./data/reduced`
- fodler `./data/weather` has extracted files from downloaded ZIP from [here](https://www.kaggle.com/selfishgene/historical-hourly-weather-data)
- folder `./data/yellow_tripdata_2014-01` has extracted file from 

The processing of one month of rides includes following steps:

1. prepare_weather - combine raw files in a single dataset (~3s)
2. mapping_taxi_rides - mapping of data to groups by: hour, day, weekday, and joining with weather (~1525s, ~25m), from 2.1Gb to 6.8Gb
3. reducing_taxi_rides - aggregates summary values and merging into less files (~159s, ~2.7m), from 6.8Gb to 123Kb
4. plot_analysis - draw charts showing correlation between time, rides, distance and weather (~2s)

## Some techniques in the solution

- slicing of a big file to smaller in memory optimized appraoch (not loading the whole huge file to memory)
- reducing files to aggregated ones for analysis
- grouping of panda dataframe by multiple columns
- applying different aggregation functions during the same grouping
- filling gaps in pandas with interpolation
