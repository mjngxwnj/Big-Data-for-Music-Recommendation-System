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
        genres VARCHAR(100),
        execution_date DATE
    );
'''

create_dim_artist = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        id VARCHAR(30) PRIMARY KEY,
        name VARCHAR(300),
        followers INT,
        popularity INT,
        link_image VARCHAR(100),
        url VARCHAR(100),
        execution_date DATE
    );
'''

create_dim_artist_genres = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        artist_id VARCHAR(30),
        genres_id VARCHAR(20),
        execution_date DATE,
        FOREIGN KEY (artist_id) REFERENCES SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_artist(id),
        FOREIGN KEY (genres_id) REFERENCES SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_genres(id)
    );
'''

create_dim_album = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        id VARCHAR(30) PRIMARY KEY,
        name VARCHAR(300),
        type VARCHAR(15),
        label VARCHAR(300),
        popularity INT,
        release_date DATE,
        copyrights TEXT,
        url VARCHAR(100),
        link_image VARCHAR(100),
        execution_date DATE
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
        time_signature INT,
        execution_date DATE
    );
'''

create_fact_track = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        track_id VARCHAR(30),
        artist_id VARCHAR(30),
        album_id VARCHAR(30),
        name VARCHAR(300),
        track_number INT,
        disc_number INT,
        duration_ms INT,
        explicit VARCHAR(10),
        url VARCHAR(100),
        restriction VARCHAR(20),
        preview VARCHAR(200),
        execution_date DATE,
        FOREIGN KEY (track_id) REFERENCES SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_track_feature(id),
        FOREIGN KEY (artist_id) REFERENCES SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_artist(id),
        FOREIGN KEY (album_id) REFERENCES SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_album(id)
    );
'''