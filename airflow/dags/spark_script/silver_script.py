from sparkIO_operations import *
from silver_class import SilverLayer
from pyspark.sql.functions import col, year
import argparse

""" Processing silver artist data. """
def silver_artist_process(spark: SparkSession, Execution_date: str):
    #read bronze artist data
    bronze_artist = read_HDFS(spark, HDFS_dir = "bronze_data/bronze_artist", file_type = 'parquet')
    bronze_artist = bronze_artist.filter(bronze_artist['Execution_date'] == Execution_date)

    #applying SilverLayer class 
    silver_artist = SilverLayer(data = bronze_artist, 
                                drop_columns       = ['Artist_Type', 'Href', 'Artist_Uri'],
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
    
    #processing data
    print("Processing for 'silver_artist' ...")
    silver_artist = silver_artist.process()
    print("Finished processing for 'silver_artist'.")
    #load data into HDFS
    write_HDFS(spark, data = silver_artist, direct = "silver_data/silver_artist", 
               file_type = 'parquet', partition = 'Execution_date')


""" Processing silver album data. """
def silver_album_process(spark: SparkSession, Execution_date: str):
    #read bronze album data
    bronze_album = read_HDFS(spark, HDFS_dir = 'bronze_data/bronze_album', file_type = 'parquet')
    bronze_album = bronze_album.filter(bronze_album['Execution_date'] == Execution_date)
    #applying Silver Layer class
    silver_album = SilverLayer(data = bronze_album,
                               drop_columns       = ['Genres', 'Available_Markets', 'Restrictions', 'Href','Uri'],
                               drop_null_columns  = ['Album_ID'],
                               fill_nulls_columns = {'Popularity': 0,
                                                     'TotalTracks': 0},
                               duplicate_columns  = ['Album_ID'],
                               rename_columns     = {'Artist': 'artist',
                                                     'Artist_ID': 'artist_id',
                                                     'Album_ID': 'id',
                                                     'Name': 'name',
                                                     'Type': 'type',
                                                     'Label': 'label',
                                                     'Popularity': 'popularity',
                                                     'Release_Date': 'release_date',
                                                     'ReleaseDatePrecision': 'release_date_precision',
                                                     'TotalTracks': 'total_tracks',
                                                     'Copyrights': 'copyrights',
                                                     'External_URL': 'url',
                                                     'Image': 'link_image'})
    
    #processing data
    print("Processing for 'silver_album' ...")
    silver_album = silver_album.process()
    print("Finished processing for 'silver_album'.")
    #load data into HDFS
    write_HDFS(spark, data = silver_album, direct = 'silver_data/silver_album', 
               file_type = 'parquet', partition = 'Execution_date')


""" Processing silver track data. """
def silver_track_process(spark: SparkSession, Execution_date: str):
    #read bronze track data
    bronze_track = read_HDFS(spark, HDFS_dir = 'bronze_data/bronze_track', file_type = 'parquet')
    bronze_track = bronze_track.filter(bronze_track['Execution_date'] == Execution_date)
    #applying Silver Layer class
    silver_track = SilverLayer(data               = bronze_track,
                               drop_columns       = ['Artists', 'Type', 'AvailableMarkets', 'Href', 'Uri', 'Is_Local'],
                               drop_null_columns  = ['Track_ID'],
                               fill_nulls_columns = {'Restrictions': 'None'},
                               duplicate_columns  = ['Track_ID'],
                               rename_columns     = {'Album_ID': 'album_id',
                                                     'Album_Name': 'album_name',
                                                     'Track_ID': 'id',
                                                     'Name': 'name',
                                                     'Track_Number': 'track_number',
                                                     'Disc_Number': 'disc_number',
                                                     'Duration_ms': 'duration_ms',
                                                     'Explicit': 'explicit',
                                                     'External_urls': 'url',
                                                     'Restrictions': 'restriction',
                                                     'Preview_url': 'preview'})
    
    #processing data
    print("Processing for 'silver_track' ...")
    silver_track = silver_track.process()
    print("Finished processing for 'silver_track'.")
    #load data into HDFS
    write_HDFS(spark, data = silver_track, direct = 'silver_data/silver_track', 
               file_type = 'parquet', partition = 'Execution_date')


""" Processing silver track feature data. """
def silver_track_feature_process(spark: SparkSession, Execution_date: str):
    #read silver track feature data
    bronze_track_feature = read_HDFS(spark, HDFS_dir = 'bronze_data/bronze_track_feature', file_type = 'parquet')
    bronze_track_feature = bronze_track_feature.filter(bronze_track_feature['Execution_date'] == Execution_date)
    #applying Silver Layer class
    silver_track_feature = SilverLayer(data              = bronze_track_feature,
                                       drop_columns      = ['Track_href', 'Type_Feature', 'Analysis_Url'],
                                       drop_null_columns = ['Track_ID'],
                                       duplicate_columns = ['Track_ID'],
                                       rename_columns    = {'Track_ID': 'id',
                                                            'Danceability': 'danceability',
                                                            'Energy': 'energy',
                                                            'Key': 'key',
                                                            'Loudness': 'loudness',
                                                            'Mode': 'mode',
                                                            'Speechiness': 'speechiness',
                                                            'Acousticness': 'acousticness',
                                                            'Instrumentalness': 'instrumentalness',
                                                            'Liveness': 'liveness',
                                                            'Valence': 'valence',
                                                            'Tempo': 'tempo',
                                                            'Time_signature': 'time_signature'})
    #processing data
    print("Processing for 'silver_track_feature' ...")
    silver_track_feature = silver_track_feature.process()
    print("Finished processing for 'silver_track_feature'.")
    #load data into HDFS
    write_HDFS(spark, data = silver_track_feature, direct = 'silver_data/silver_track_feature', 
               file_type = 'parquet', partition = 'Execution_date')


#main call
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Current date argument")
    parser.add_argument('--execution_date', required = False, help = "execution_date")
    args = parser.parse_args()

    with get_sparkSession("Silver_task_spark") as spark:
        print("------------------------------- Silver task starts! -------------------------------")
        print("Starting silver artist data processing...")
        silver_artist_process(spark, args.execution_date)
        print("Starting silver album data processing...")
        silver_album_process(spark, args.execution_date)
        print("Starting silver track data processing...")
        silver_track_process(spark, args.execution_date)
        print("Starting silver track feature data processing...")
        silver_track_feature_process(spark, args.execution_date)
        print("------------------------------ Silver task finished! -------------------------------")