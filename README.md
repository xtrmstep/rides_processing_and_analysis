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

- Number of rides by weather conditions
- Number of rides per day, hour or weekday
- Number of rides by district, by day, hour, weekday, and by weather conditions
- Distance by weather conditions
- Number of rides per disctrict (not implemented)

## Processing order

_Note: before processing make sure the folder `./data/processing` is empty._

It processes 2.1Gb (one month of data):

1. prepare_weather - from downloaded files, creates a single CSV file with all properties (~3s) 
2. mapping_taxi_rides - from downloaded file, creates multiple files with grouping by: day+hour, day, weekday (~791s, ~13m), increasing 2.1Gb of original data to 3.4Gb (+1.3Gb, +62%)
3. join_rides_with_weather - adds information about weather to all files (~666s, ~11m), increasing 3.4Gb to 5.1Gb (+1.7Gb, +50%)
4. reducing_taxi_rides (~113s, ~2m), decreasing 5.1Gb to 328.4Kb
5. plot_analysis ()
