# Big data for Music Recommendation System
## Table of Contents
- [Introduction](#introduction)
- [Project Overview](#project-overview)
## Introduction 
In this day and age, music is an essential part of life, offering both entertainment and emotional connection. Our team aims to create an end-to-end date pipeline architecture that covers data collection, processing, storage, analysis, reporting, and building a recommendation system for music based on user input.

### Data sources
The data source is initially collected from https://kworb.net/itunes/extended.html, which includes top 15000 artist names that will change daily. After that, we use Spotify API to retrieve data about artist's information, albums, tracks and track features based on the list of artist names from Kworb website.

### Architecture
![My Image](./images/Architecture.png)

### Tools
- **Python**: Main programming language.
- **Docker**: Run containers, ensuring consistent and scalable environments.
- **MongoDB**: Used for data storage as Database
- **HDFS**: A part of Hadoop architecture, used for data storage as Data Lake.
- **Snowflake**: Cloud-Based Data Warehouse.
- **PowerBI**: A tool for displaying data and providing comprehensive overview.
- **Airflow**: A framework that uses Python to schedule and run tasks.
### Directory Structure
![directory](https://github.com/mjngxwnj/Big-Data-for-Music-Recommendation-System/blob/main/images/directories.PNG)
## Project Overview
### Data Collection and Ingestion
The data collection and ingestion process involves retrieving information from **Kworb.net** and **Spotify API**, then storing it in **MongoDB**.
#### 1. Retrieving Data from Kworb.net
- Use `pandas.read_html(url)` to extract tables from the website.
- Select the first table and extract two columns: `Pos` (ranking position) and `Artist` (artist name).
- Store the list of **15,000** artists in **MongoDB**.
#### 2. Fetching Artist Information from Spotify API
- Use Spotipy to connect to the **Spotify API** with **Client ID** and **Client Secret**.
- Call `sp.search ` to retrieve artist details by name and store the data in **MongoDB**.
#### 3. Retrieving Album and Track Information
- Use the artist ID to fetch a list of album IDs via sp.artist_albums.
- Split the album list into smaller chunks to optimize API calls.
- Use `sp.album` to retrieve 20 albums and their tracks in a single API request.
- Store album and track data in MongoDB.
#### 4. Fetching Track Features
- Retrieve track IDs from the previous step to fetch track feature data.
- Split the track ID list into smaller chunks.
- Use `sp.audio_feature` to retrieve 100 track features per API request.
- Store the extracted data in MongoDB.
  
  ![crawl_api](https://github.com/mjngxwnj/Big-Data-for-Music-Recommendation-System/blob/main/images/crawl_api.jpg)
### Daily Data Scraping and Storing Strategy
#### 1. Initial Data Scraping and Storing
- Since calling the **Spotify API** for **15,000** artists at once risks exceeding limits, the process is split over 3 days (**5,000 artists/day**). Data is first saved in **CSV files** before being loaded into **MongoDB** as the initial dataset.
#### 2. Subsequent Data Scraping and Storing
- To avoid duplicate data, each day's new **15,000** artist names from **Kworb.net** are compared with the existing **15,000** artists in **MongoDB** using a **Left Anti Join**. Only new artists are processed via the **Spotify API**, ensuring efficient updates.
  
  ![left_anti_join](https://github.com/mjngxwnj/Big-Data-for-Music-Recommendation-System/blob/main/images/leftanti_join_artistname.png)
- This strategy ensures that we only fetch **new artists** to call the **Spotify API** and retrieve album and track information, minimizing duplicates and reducing API requests. (After performing the **Left Anti Join**, the number of daily artist names is around **3,000**).
- New artist names are stored in **MongoDB**, then retrieved to call the **Spotify API** to fetch artist, album, and track data, which is also stored in **MongoDB**. We add an **execute_date** column to track the data execution date and ensure that only the latest data is used for API calls, preventing redundant requests for past data.
  
  ![daily_crawl](https://github.com/mjngxwnj/Big-Data-for-Music-Recommendation-System/blob/main/images/daily_crawl_data.png)
### Three-Layer Data Lake Processing
#### 1. Three-Layer Data Lake Processing
We use HDFS (Hadoop Distributed File System) to store processed and transformed datasets, forming our Data Lake.
#### 2. Data Lake Architecture
Our data lake processing system is designed with three main layers: **Bronze, Silver, and Gold**. Each layer plays a crucial role in storing and processing data at different levels for **analysis**, **reporting**, and **Machine Learning model building**.
- The defined schemas (PySpark Schema) are structured as follows:
```python
""" Function for getting schemas. """
def get_schema(table_name: str) -> StructType:
    """ Artist schema. """
    artist_schema = [StructField('Artist_ID',     StringType(), True),
                    StructField('Artist_Name',    StringType(), True),
                    StructField('Genres',         ArrayType(StringType(), True), True),
                    StructField('Followers',      IntegerType(), True),
                    StructField('Popularity',     IntegerType(), True),
                    StructField('Artist_Image',   StringType(), True),
                    StructField('Artist_Type',    StringType(), True),
                    StructField('External_Url',   StringType(), True),
                    StructField('Href',           StringType(), True),
                    StructField('Artist_Uri',     StringType(), True),
                    StructField('Execution_date', DateType(), True)]
    #applying struct type
    artist_schema = StructType(artist_schema)
```
