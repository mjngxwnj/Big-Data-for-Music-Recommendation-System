import streamlit as st
import snowflake.connector
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkConf



def search_track_Snowflake(song_name):   
    conn = snowflake.connector.connect(**snowflake_config)
    query = f"""SELECT DIM_ARTIST.NAME ARTIST_NAME, FACT_TRACK.URL URL, 
    FOLLOWERS, TRACK_ID, FACT_TRACK.NAME TRACK_NAME, PREVIEW, LINK_IMAGE
    FROM DIM_ARTIST JOIN FACT_TRACK 
    ON DIM_ARTIST.ID = FACT_TRACK.ARTIST_ID
    WHERE FACT_TRACK.NAME ILIKE '{song_name}%'
    ORDER BY FOLLOWERS DESC;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetch_pandas_all()
    cursor.close()
    conn.close()

    return data








def search_rcm_mood_genres(mood: str, genres: str):
    conn = snowflake.connector.connect(**snowflake_config)

    query = f"""
    SELECT * FROM RCM_MOOD_GENRES_TABLE
    WHERE GENRES = '{genres}' AND MOOD = '{mood}'
    LIMIT 10;
    """

    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetch_pandas_all()
    cursor.close()
    conn.close()

    return data


def search_rcm_bcf(song_name: str): 

    
    spark = SparkSession.builder.config(conf = conf).getOrCreate()
    HDFS_PATH = "hdfs://namenode:9000/datalake/models/rcm_bcf_model/"
    data = spark.read.format('parquet').option('header', 'true').load(HDFS_PATH)
    data.cache()

    data.createOrReplaceTempView("songs")
    query = f""" SELECT * FROM songs WHERE LOWER(name) LIKE LOWER('{song_name}%') """ 
    songs = spark.sql(query)

    song_list = []
    songs = songs.rdd.map(lambda row: row.asDict()).collect()

    for row in songs:
        song_list.append({'artist_name': row['artist_name'],
                          'followers': row['followers'],
                          'link_image': row['link_image'],
                          'url': row['url'],
                          'name': row['name'],
                          'preview': row['preview']})

    return song_list



class BackEnd():
    def __init__(self):
        #for snowflake config
        self.snowflake_config_rcm_mood = {
            'user': 'HuynhThuan',
            'password': 'Thuan123456',
            'account': 'sl70006.southeast-asia.azure',
            'warehouse': 'COMPUTE_WH',
            'database': 'SPOTIFY_RCM_DB',
            'schema': 'SPOTIFY_RCM_SCHEMA'
        }
        self.snowflake_config_dataset = {
            'user': 'HuynhThuan',
            'password': 'Thuan123456',
            'account': 'sl70006.southeast-asia.azure',
            'warehouse': 'COMPUTE_WH',
            'database': 'SPOTIFY_MUSIC_DB',
            'schema': 'SPOTIFY_MUSIC_SCHEMA'
        }

        #for spark session
        HDFS_RCM_CBF_PATH = "hdfs://namenode:9000/datalake/models/rcm_bcf_model/"
        conf = SparkConf()
        conf = conf.setAppName("model_spark") \
                .setMaster("local") \
                .set("spark.executor.memory", "4g") \
                .set("spark.executor.cores", "2")
        self.spark = SparkSession.builder.config(conf = conf).getOrCreate()
        self.data = self.spark.read.format('parquet').option('header', 'true').load(HDFS_RCM_CBF_PATH)
        self.data.cache()


    def rea