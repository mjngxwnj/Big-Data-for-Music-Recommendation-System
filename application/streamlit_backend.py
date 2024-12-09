import streamlit as st
import snowflake.connector
import pandas as pd

snowflake_config = {
    'user': 'HuynhThuan',
    'password': 'Thuan123456',
    'account': 'sl70006.southeast-asia.azure',
    'warehouse': 'COMPUTE_WH',
    'database': 'SPOTIFY_RCM_DB',
    'schema': 'SPOTIFY_RCM_SCHEMA'
}

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
