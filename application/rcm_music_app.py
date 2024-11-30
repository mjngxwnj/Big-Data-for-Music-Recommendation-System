import streamlit as st
import snowflake.connector
import pandas as pd
def get_song(song_name):   
    conn = snowflake.connector.connect(
        user='HuynhThuan',
        password='Thuan123456',
        account='sl70006.southeast-asia.azure',
        warehouse='COMPUTE_WH',
        database='SPOTIFY_MUSIC_DB',
        schema='SPOTIFY_MUSIC_SCHEMA'
    )

    query = f""" SELECT DIM_ARTIST.NAME, PREVIEW, LINK_IMAGE
    FROM DIM_ARTIST JOIN FACT_TRACK 
    ON DIM_ARTIST.ID = FACT_TRACK.ARTIST_ID
    WHERE FACT_TRACK.NAME = '{song_name}' LIMIT 1;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    data = pd.DataFrame(rows, columns=columns)
    cursor.close()
    conn.close()
    return data

song_name = st.text_input("Nhập tên bài hát:")

if song_name:
    data = get_song(song_name)
    st.markdown(f"{data.iloc[0]['NAME']}")
    st.image(data.iloc[0]['LINK_IMAGE'])
    st.audio(data.iloc[0]['PREVIEW'])