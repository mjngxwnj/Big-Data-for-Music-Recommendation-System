import streamlit as st
import snowflake.connector
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
import atexit


class BackEnd: 
    """ ============================================== INIT ============================================== """
    def __init__(self):
        self._snowflake_config_rcm_db = {
            'user': 'HuynhThuan',
            'password': 'Thuan123456',
            'account': 'sl70006.southeast-asia.azure',
            'warehouse': 'COMPUTE_WH',
            'database': 'SPOTIFY_RCM_DB',
            'schema': 'SPOTIFY_RCM_SCHEMA'
        }
        self._snowflake_config_music_db = {
            'user': 'HuynhThuan',
            'password': 'Thuan123456',
            'account': 'sl70006.southeast-asia.azure',
            'warehouse': 'COMPUTE_WH',
            'database': 'SPOTIFY_MUSIC_DB',
            'schema': 'SPOTIFY_MUSIC_SCHEMA'
        }
        self._HDFS_RCM_CBF_PATH = "hdfs://namenode:9000/datalake/models/rcm_bcf_model/"
        
        if "conn_music_db" not in st.session_state:
            st.session_state.conn_music_db = snowflake.connector.connect(**self._snowflake_config_music_db)
        
        if "conn_rcm_db" not in st.session_state:
            st.session_state.conn_rcm_db = snowflake.connector.connect(**self._snowflake_config_rcm_db)

        if "spark" not in st.session_state:
            conf = SparkConf()
            conf = conf.setAppName("model_spark") \
                    .setMaster("local") \
                    .set("spark.executor.memory", "4g") \
                    .set("spark.executor.cores", "2")
            st.session_state.spark = SparkSession.builder.config(conf = conf).getOrCreate()

        if "rcm_bcf_data" not in st.session_state:
            st.session_state.rcm_bcf_data = st.session_state.spark.read.format('parquet') \
                                                                  .option('header', 'true') \
                                                                  .load(self._HDFS_RCM_CBF_PATH)

        self._conn_music_db = st.session_state.conn_music_db
        self._conn_rcm_db = st.session_state.conn_rcm_db
        self._spark = st.session_state.spark
        self._rcm_bcf_data = st.session_state.rcm_bcf_data

        atexit.register(self.clean)

    def clean(self):
        self._conn_music_db.close()
        self._conn_rcm_db.close()
        self._spark.stop()

    """ ============================================ EXECUTE & QUERY ============================================ """
    def read_music_db(self, song_name: str = None, artist_name: str = None):
        cursor = self._conn_music_db.cursor()
        query = f""" SELECT DIM_ARTIST.NAME ARTIST_NAME, FACT_TRACK.URL URL, 
                    FOLLOWERS, TRACK_ID, FACT_TRACK.NAME TRACK_NAME, PREVIEW, LINK_IMAGE
                    FROM DIM_ARTIST JOIN FACT_TRACK 
                    ON DIM_ARTIST.ID = FACT_TRACK.ARTIST_ID
                    WHERE FACT_TRACK.NAME ILIKE '{song_name}%'
                    ORDER BY FOLLOWERS DESC; 
                """
        cursor.execute(query)
        songs = cursor.fetch_pandas_all()
        cursor.close()
        return songs

    #def exec_rcm_by_album(self, track_id):

    def exec_rcm_bcf(self, track_id: str):
        df = self._rcm_bcf_data
        track_list = df.filter(df["track_id"] == track_id)
        track_list.cache()

        track = track_list.first()
        input_song_name, input_features = track['name'], track['normalized_features']

        genres_list = track_list.select("genres").rdd.flatMap(lambda x: x).filter(lambda genres: genres != "").collect()
        filtered_songs = df.filter(df['genres'].isin(genres_list))
        filtered_songs = filtered_songs.drop('genres').dropDuplicates(['track_id'])

        def cosine_similarity(v1, v2):
            dot_product = float(v1.dot(v2))
            norm_v1 = float(v1.norm(2))
            norm_v2 = float(v2.norm(2))
            return dot_product / (norm_v1 * norm_v2)

        cosine_similarity_udf = udf(lambda x: cosine_similarity(x, input_features), FloatType())
        top_recommendations = filtered_songs.withColumn("similarity_scores", cosine_similarity_udf(filtered_songs['normalized_features'])) \
                                            .orderBy("similarity_scores", ascending= False).limit(10) \
                                            .select("artist_name", "followers", "name", "similarity_scores", 
                                                     "track_id", "link_image", "url", "preview") \
                                            .collect()
        songs = []
        for song in top_recommendations:
            songs.append({'artist_name': song['artist_name'],
                            'followers': song['followers'],
                            'link_image': song['link_image'],
                            'url': song['url'],
                            'name': song['name'],
                            'preview': song['preview']})
        return songs
    






# def search_track_Snowflake(song_name):   
#     conn = snowflake.connector.connect(**snowflake_config)
#     query = f"""SELECT DIM_ARTIST.NAME ARTIST_NAME, FACT_TRACK.URL URL, 
#     FOLLOWERS, TRACK_ID, FACT_TRACK.NAME TRACK_NAME, PREVIEW, LINK_IMAGE
#     FROM DIM_ARTIST JOIN FACT_TRACK 
#     ON DIM_ARTIST.ID = FACT_TRACK.ARTIST_ID
#     WHERE FACT_TRACK.NAME ILIKE '{song_name}%'
#     ORDER BY FOLLOWERS DESC;
#     """
#     cursor = conn.cursor()
#     cursor.execute(query)
#     data = cursor.fetch_pandas_all()
#     cursor.close()
#     conn.close()

#     return data

# def search_rcm_mood_genres(mood: str, genres: str):
#     conn = snowflake.connector.connect(**snowflake_config)

#     query = f"""
#     SELECT * FROM RCM_MOOD_GENRES_TABLE
#     WHERE GENRES = '{genres}' AND MOOD = '{mood}'
#     LIMIT 10;
#     """

#     cursor = conn.cursor()
#     cursor.execute(query)
#     data = cursor.fetch_pandas_all()
#     cursor.close()
#     conn.close()

#     return data


# def search_rcm_bcf(song_name: str): 
#     spark = SparkSession.builder.config(conf = conf).getOrCreate()
#     HDFS_PATH = "hdfs://namenode:9000/datalake/models/rcm_bcf_model/"
#     data = spark.read.format('parquet').option('header', 'true').load(HDFS_PATH)
#     data.cache()

#     data.createOrReplaceTempView("songs")
#     query = f""" SELECT * FROM songs WHERE LOWER(name) LIKE LOWER('{song_name}%') """ 
#     songs = spark.sql(query)

#     song_list = []
#     songs = songs.rdd.map(lambda row: row.asDict()).collect()

#     for row in songs:
#         song_list.append({'artist_name': row['artist_name'],
#                           'followers': row['followers'],
#                           'link_image': row['link_image'],
#                           'url': row['url'],
#                           'name': row['name'],
#                           'preview': row['preview']})

#     return song_list





#     """ ============================================== CACHE DATA ============================================== """
#     @st.cache_resource
#     def get_conn_music_db(_self):
#         conn = snowflake.connector.connect(**_self._snowflake_config_music_db)
#         return conn.cursor()
    
#     @st.cache_resource
#     def get_conn_rcm_db(_self):
#         conn = snowflake.connector.connect(**_self._snowflake_config_rcm_db)
#         return conn.cursor()

#     @st.cache_data
#     def get_rcm_bcf_dataset(_self):
#         song_list = []
#         songs = _self._spark.read.format('parquet').option('header', 'true').load(_self._HDFS_RCM_CBF_PATH).limit(10).collect()
#         for row in songs:
#             song_list.append({'artist_name': row['artist_name'],
#                             'followers': row['followers'],
#                             'link_image': row['link_image'],
#                             'url': row['url'],
#                             'name': row['name'],
#                             'preview': row['preview']})
#         return song_list