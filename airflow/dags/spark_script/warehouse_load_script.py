from sparkIO_operations import *
import argparse

def load_data_Snowflake(Execution_date: str):
    with get_sparkSession("snowflake_load_data_spark") as spark:
        dim_artist = read_HDFS(spark, HDFS_dir = 'gold_data/dim_artist', file_type = 'parquet')
        dim_artist = dim_artist.filter(dim_artist['Execution_date'] == Execution_date)
        write_SnowFlake(spark, data = dim_artist, table_name = 'SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_artist')

        dim_genres = read_HDFS(spark, HDFS_dir = 'gold_data/dim_genres', file_type = 'parquet')
        dim_genres = dim_genres.filter(dim_genres['Execution_date'] == Execution_date)
        write_SnowFlake(spark, data = dim_genres, table_name = 'SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_genres')

        dim_artist_genres = read_HDFS(spark, HDFS_dir = 'gold_data/dim_artist_genres', file_type = 'parquet')
        dim_artist_genres = dim_artist_genres.filter(dim_genres['Execution_date'] == Execution_date)
        write_SnowFlake(spark, data = dim_artist_genres, table_name = 'SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_artist_genres')

        dim_album = read_HDFS(spark, HDFS_dir = 'gold_data/dim_album', file_type = 'parquet')
        dim_album = dim_album.filter(dim_album['Execution_date'] == Execution_date)
        write_SnowFlake(spark, data= dim_album, table_name = 'SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_album')

        dim_track_feature = read_HDFS(spark, HDFS_dir = 'gold_data/dim_track_feature', file_type = 'parquet')
        dim_track_feature = dim_track_feature.filter(dim_track_feature['Execution_date'] == Execution_date)
        write_SnowFlake(spark, data = dim_track_feature, table_name = 'SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.dim_track_feature')

        fact_track = read_HDFS(spark, HDFS_dir = 'gold_data/fact_track', file_type = 'parquet')
        fact_track = fact_track.filter(fact_track['Execution_date'] == Execution_date)
        write_SnowFlake(spark, data = fact_track, table_name = 'SPOTIFY_MUSIC_DB.SPOTIFY_MUSIC_SCHEMA.fact_track')
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Current date argument")
    parser.add_argument('--execution_date', required = False, help = "execution_date")
    args = parser.parse_args()

    print("------------------------------- Warehouse load task starts! -------------------------------")
    load_data_Snowflake(args.execution_date)
    print("------------------------------ Warehouse load task finished! -------------------------------")
