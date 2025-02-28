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
We use **HDFS (Hadoop Distributed File System)** to store processed and transformed datasets, collectively referred to as the **Data Lake**.
Our **Data Lake Processing System** consists of three main layers: **Bronze, Silver, and Gold**. Each layer plays a critical role in storing and refining data for analysis, reporting, and Machine Learning.
#### 1. Bronze Layer Processing
At this stage, data is extracted from **MongoDB** after being collected from the **Spotify API**. This includes details about **artists, albums, tracks, and track features**. 
- The defined schemas (PySpark Schema) will be structured as follows:
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

  Similarly, schemas will be defined for the Album, Track, and Track Feature tables.
- Additionally, during this process, we will apply an **Incremental Load strategy** based on the Execution_date column to load data daily. This helps minimize the amount of data that needs to be processed, transformed, and loaded.
  
  ![incremental_load](https://github.com/mjngxwnj/Big-Data-for-Music-Recommendation-System/blob/main/images/incremental_load.png)
  We can see that only the data crawled on a specific day is read, processed, and stored in the Data Lake (data from previous days is not read, processed, or stored).
#### 2. Silver Layer Processing
In this stage, data is read from the **Bronze Layer Data Storage** (where data has been minimally processed and schema is applied to standardize column data types). The following data processing steps are applied:

- **Drop Columns**: Remove unnecessary columns.
- **Drop Null Columns**: Drop rows containing null values based on selected subset columns.
- **Fill Null**: Replace null values with specific values.
- **Drop Duplicates**: Remove duplicate rows based on selected subset columns.
- **Handle Nested Data**: Process rows with nested structures.
- **Rename Columns**: Rename columns as needed.

To simplify management and avoid repetitive data processing tasks for each table, we will create a `SilverLayer` class. This class will apply the above data processing steps for each dataset.

For each table requiring processing, you simply need to apply this class and pass the necessary parameters such as the dataset, list of columns to drop, columns to rename, and the subset column to drop null values.
```python
""" Create SilverLayer class to process data in the Silver layer. """
class SilverLayer:
    #init 
    def __init__(self, data: pyspark.sql.DataFrame, 
                 drop_columns: list = None, 
                 drop_null_columns: list = None,
                 fill_nulls_columns: dict = None,
                 duplicate_columns: list = None,
                 nested_columns: list = None,
                 rename_columns: dict = None,
                 ):
"""Initialize class attributes for data processing."""
        self._data = data
        self._drop_columns = drop_columns
        self._drop_null_columns = drop_null_columns
        self._fill_nulls_columns = fill_nulls_columns
        self._duplicate_columns = duplicate_columns
        self._nested_columns = nested_columns
        self._rename_columns = rename_columns


    """ Method to drop unnecessary columns. """
    def drop(self):
        self._data = self._data.drop(*self._drop_columns)

    """ Method to drop rows based on null values in each column. """
    ...

    """ Method to fill null values. """
    ...
```
After creating the SilverLayer class, we will read data from the Bronze Layer, apply this class to process the data, and then load the data into the Silver Layer Storage.

The data processing function in the Silver Layer:
```python
silver_artist = SilverLayer(data = bronze_artist, 
                            drop_columns       = ['Artist_Type', 'Href', 
                                                  'Artist_Uri', 'Execution_date'],
                            drop_null_columns  = ['Artist_ID'], 
                            fill_nulls_columns = {'Followers': 0,
                                                  'Popularity': 0},
                            duplicate_columns  = ['Artist_ID'],
                            nested_columns     = ['Genres'],
                            rename_columns     = {'Artist_ID': 'id',
                                                  'Artist_Name': 'name',
                                                  'Genres': 'genres',
                                                  'Followers': 'followers',
                                                  'Popularity': 'popularity',
                                                  'Artist_Image': 'link_image',
                                                  'External_Url': 'url'})
    
    """ Call the process method of SilverLayer to perform data processing """
    print("Processing for 'silver_artist' ...")
    silver_artist = silver_artist.process()
    print("Finished processing for 'silver_artist'.")
```
#### 3. Gold Layer Processing
At this stage, after the data has been processed in the Silver Layer, we will perform the process of combining tables to create a schema that follows the Snowflake structure, normalized to the highest level—3NF. In this schema, the fact table will be the track table, and this schema will be applied to organize the data in the Data Warehouse.

- This is the schema we aim to achieve:
  
  ![schema](https://github.com/mjngxwnj/Big-Data-for-Music-Recommendation-System/blob/main/images/schema.jpg)
  
In this schema, we will have 6 tables:
- **Fact Track**: This is the main table of the dataset, containing information about all the tracks, with foreign key columns linking to the primary keys of the other tables.  
- **Dim Artist**: This is the table containing data about the artists and their corresponding information.  
- **Dim Album**: This table contains data about albums, including details like name, copyright, release date, etc.  
- **Dim Track Feature**: This table holds data about the features of a track, including loudness, mode, tempo, etc.  
- **Dim Genres**: This table contains information about the genres of music, such as pop, rock, etc.  
- **Dim Artist-Genres**: Since the relationship between artists and genres is many-to-many (an artist can have multiple genres, and a genre can belong to multiple artists), the dim_artist_genres table acts as a bridge between the two.
  
Thus, we will take the cleaned data from the Silver Layer Storage and perform several join and aggregation operations to obtain a dataset organized according to the schema we have prepared. Through the steps of joining tables and removing unnecessary columns, while also creating primary and foreign key columns for each table, we will save the entire dataset into the Gold Layer Storage.  
#### 4. Data Warehouse Storing
- In this project, the chosen Data Warehouse will be Snowflake, a cloud-based data warehouse. Snowflake is quite powerful for storing clean data, reporting, and analytics.
- Once the dataset in the Gold Layer is completed, we will need to initialize the Database, Schema, and Tables in the Snowflake Data Warehouse. Then, we will load the entire data from the Gold Layer into Snowflake.
- After loading the entire dataset into Snowflake, we will log into the Snowflake account and we will see the tables that have been uploaded.
  ![snowflake](https://github.com/mjngxwnj/Big-Data-for-Music-Recommendation-System/blob/main/images/snowflake.PNG)
  ![data_warehouse](https://github.com/mjngxwnj/Big-Data-for-Music-Recommendation-System/blob/main/images/data_warehouse.PNG)
