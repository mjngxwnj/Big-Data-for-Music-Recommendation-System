"""-------------------------CREATE DATABASE AND SCHEMA-------------------------"""
#create database
create_spotify_music_db = '''
    CREATE DATABASE IF NOT EXISTS {{ params.database_name}};
'''

#create schema
create_spotify_music_schema = '''
    CREATE SCHEMA IF NOT EXISTS {{ params.schema_name}};
'''


"""-------------------------CREATE TABLE-------------------------"""
create_dim_genres = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        id VARCHAR(20) PRIMARY KEY,
        genres VARCHAR(200)
    );
'''

create_dim_artist = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        id VARCHAR(30) PRIMARY KEY,
        name VARCHAR(400),
        followers INT,
        popularity INT,
        link_image VARCHAR(200),
        url VARCHAR(200)
    );
'''

create_dim_artist_genres = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        artist_id VARCHAR(30),
        genres_id VARCHAR(20),
        FOREIGN KEY (artist_id) REFERENCES SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_artist(id),
        FOREIGN KEY (genres_id) REFERENCES SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_genres(id)
    );
'''

create_dim_album = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        id VARCHAR(30) PRIMARY KEY,
        name VARCHAR(400),
        type VARCHAR(100),
        label VARCHAR(400),
        popularity INT,
        release_date DATE,
        copyrights TEXT,
        url VARCHAR(200),
        link_image VARCHAR(200)
    );
'''

create_dim_track_feature = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        id VARCHAR(30) PRIMARY KEY,
        danceability FLOAT,
        energy FLOAT,
        key INT,
        loudness FLOAT,
        mode INT,
        speechiness FLOAT,
        acousticness FLOAT,
        instrumentalness FLOAT,
        liveness FLOAT,
        valence FLOAT,
        tempo FLOAT,
        time_signature INT
    );
'''

create_fact_track = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        track_id VARCHAR(30),
        artist_id VARCHAR(30),
        album_id VARCHAR(30),
        name VARCHAR(400),
        track_number INT,
        disc_number INT,
        duration_ms INT,
        explicit VARCHAR(10),
        url VARCHAR(200),
        restriction VARCHAR(100),
        preview VARCHAR(200),
        FOREIGN KEY (track_id) REFERENCES SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_track_feature(id),
        FOREIGN KEY (artist_id) REFERENCES SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_artist(id),
        FOREIGN KEY (album_id) REFERENCES SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_album(id)
    );
'''