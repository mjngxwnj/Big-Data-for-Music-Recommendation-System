# Big data for Music Recommendation System
## Table of Contents
1. [Introduction](#1.-introduction)  
2. [Data Collection and Ingestion](#2.-data-collection-and-ingestion)  
    2.1 [Web Scraping and API Data Fetching](#2.1-web-scraping-and-api-data-fetching)
## 1. Introduction 
- In this day and age, music is an essential part of life, offering both entertainment and emotional connection. Our team aims to create an end-to-end date pipeline architecture that covers data collection, processing, storage, analysis, reporting, and building a recommendation system for music based on user input.

- **Data sources**: The data source is initially collected from https://kworb.net/itunes/extended.html, which includes top 15000 artist names that will change daily. After that, we use Spotify API to retrieve data about artist's information, albums, tracks and track features based on the list of artist names from Kworb website.

- **Tools**:
    - **Python**: Main programming language.
    - **Docker**: Run containers, ensuring consistent and scalable environments.
    - **MongoDB**: Used for data storage as Database
    - **HDFS**: A part of Hadoop architecture, used for data storage as Data Lake.
    - **Snowflake**: Cloud-Based Data Warehouse.
    - **PowerBI**: A tool for displaying data and providing comprehensive overview.
    - **Airflow**: A framework that uses Python to schedule and run tasks.

- **Architecture**:    
![My Image](./images/Architecture.png)
- **Link**: To explore the full source code, feel free to check out our GitHub repository:  
*https://github.com/mjngxwnj/Big-Data-for-Music-Recommendation-System*

## 2. Data Collection and Ingestion  
### 2.1 Web Scraping and API Data Fetching  
To be continued ...