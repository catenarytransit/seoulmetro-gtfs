# Seoul Metro GTFS

This is the Seoul Metro GTFS generator for Catenary Maps, it automatically generates a GTFS (General Transit Feed Specification) feed for the Seoul Metro subway system. It is built with Rust to ensure high performance and reliability during the data scraping and generation process.

## Technical Explanation

1.  **Data Acquisition**:
    -   **Station Metadata**: Retrieves the complete list of stations and lines from the Seoul Metro official website (`http://www.seoulmetro.co.kr`).
    -   **Geospatial Data**: Matches station names against a specific OpenStreetMap (OSM) PBF extract (`stations.osm.pbf`) to obtain accurate latitude and longitude coordinates. It employs a fallback mechanism using Wikipedia for any stations not found in OSM.
    -   **Timetables**: We scrape all departing trips from each station, then scrape all the unique train number sequences to identify the correct stop times for each trip.

2.  **Data Processing**:
    -   **Normalisation**: Cleans and normalises station names and line data to ensure consistency across different data sources.
    -   **Schedule Assembly**: We stitch together individual station stops into complete trips (`trips.txt` and `stop_times.txt`), handling complex logic such as service days and midnight crossings.

3.  **GTFS Generation**:
    -   Outputs standard GTFS files into a gtfs_output folder. You can zip it into gtfs.zip later

## Automatic Daily Updates

The GTFS feed is updated automatically every day by GitHub Actions runners. This automated workflow builds the project, scrapes the latest data, generates the feed, and publishes it as a release asset.

## Download

The latest version of the GTFS feed is available for download here:
[https://github.com/catenarytransit/seoulmetro-gtfs/releases/download/latest/gtfs.zip](https://github.com/catenarytransit/seoulmetro-gtfs/releases/download/latest/gtfs.zip)
