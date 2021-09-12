# Datasets

taxi trips https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2014, January, Yellow Taxi Trip Records

weather https://www.kaggle.com/selfishgene/historical-hourly-weather-data
hourly data from 2012 to 2017 (take for 2014)

visualization
dash https://realpython.com/python-dash/
https://pythonprogramming.net/deploy-vps-dash-data-visualization/

## Analyzing the effect of weather on ridership

Types of dependencies:

- Number and distance of rides by weather conditions
- Number and distance of rides per day, hour, weekday

## Processing order

_Note: before processing make sure the folder `./data/processing` is empty._

It processes 2.1Gb (one month of data):

1. prepare_weather - from downloaded files, creates a single CSV file with all properties (~3s)
2. mapping_taxi_rides - from downloaded file, creates multiple files with grouping by: day+hour, day, weekday and joining with weather (~1525s, ~25m), from 2.1Gb to 6.8Gb
3. reducing_taxi_rides - calculate aggregates in every file and merge into single files (~159s, ~2.7m), from 6.8Gb to 123Kb
4. plot_analysis - draw charts showing correlation between time, rides, distance and weather (~2s)

## Techniques in the solution

- slicing of a big file ot smaller in memory optimized appraoch (not loading the whole huge file to memory)
- reducing files to aggregated ones for analysis
- grouping of panda dataframe by multiple columns
- applying different aggregation functions during the same grouping
