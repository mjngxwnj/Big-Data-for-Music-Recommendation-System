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
        id VARCHAR(10) PRIMARY KEY,
        genres VARCHAR(30)
    );
'''

create_dim_artist = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        id VARCHAR(30) PRIMARY KEY,
        name VARCHAR(50),
        followers INT,
        popularity INT,
        link_image VARCHAR(100),
        url VARCHAR(100)
    );
'''

create_dim_artist_genres = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        artist_id VARCHAR(30),
        genres_id VARCHAR(10)
    );
'''

create_dim_album = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        id VARCHAR(30) PRIMARY KEY,
        name VARCHAR(50),
        type VARCHAR(15),
        label VARCHAR(50),
        popularity INT,
        release_date DATE,
        copyrights VARCHAR(200),
        url VARCHAR(100),
        link_image VARCHAR(100)
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
        name VARCHAR(100),
        track_number INT,
        disc_number INT,
        duration_ms INT,
        explicit VARCHAR(10),
        url VARCHAR(100),
        restriction VARCHAR(20),
        preview VARCHAR(200)
    );
'''