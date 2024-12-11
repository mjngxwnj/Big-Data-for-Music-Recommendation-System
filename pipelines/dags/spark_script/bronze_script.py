from sparkIO_operations import *
from spark_schemas import get_schema
from pyspark.sql.functions import split, col, get_json_object, to_date, regexp_replace, length
import argparse

""" Applying schemas and loading data from MongoDB into HDFS."""
def bronze_layer_processing(Execution_date: str):
    #get spark Session
    with get_sparkSession(appName = 'Bronze_task_spark') as spark:
        """------------------------ BRONZE ARTIST ------------------------"""
        artist_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'artist_collection')
        artist_data = artist_data.filter(artist_data['Execution_date'] == Execution_date)

        print("Starting bronze preprocessing for artist data...")
        #preprocessing before loading data
        try:
            artist_data = artist_data.withColumn('Genres', split(col('Genres'), ",")) \
                                     .withColumn('Followers', col('Followers').cast('int')) \
                                     .withColumn('Popularity', col('Popularity').cast('int')) \
                                     .withColumn('External_Url', get_json_object(col('External_Url'),'$.spotify')) \
                                     .withColumn('Execution_date', col('Execution_date').cast('date'))
            #reorder columns after reading 
            artist_data = artist_data.select('Artist_ID', 'Artist_Name', 'Genres', 
                                            'Followers', 'Popularity', 'Artist_Image', 
                                            'Artist_Type', 'External_Url', 'Href', 'Artist_Uri', 'Execution_date')
            #applying schema        
            artist_data = spark.createDataFrame(artist_data.rdd, schema = get_schema('artist'))

            print("Finished bronze preprocessing for artist data.")

            #upload data into HDFS
            write_HDFS(spark, data = artist_data, direct = 'bronze_data/bronze_artist', 
                       file_type = 'parquet', mode = "append", partition = 'Execution_date')
        except Exception as e:
            print(f"An error occurred while preprocessing bronze data: {e}")

        """------------------------ BRONE ALBUM ------------------------"""
        album_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'album_collection')
        album_data = album_data.filter(album_data['Execution_date'] == Execution_date)
        print("Starting bronze preprocessing for album data...")
        try:
            album_data = album_data.withColumn('Popularity', col('Popularity').cast('int')) \
                                   .withColumn('Genres', split(col('Genres'), ",")) \
                                   .withColumn('Release_Date', to_date('Release_Date', "MM/dd/yyyy")) \
                                   .withColumn('TotalTracks', col('TotalTracks').cast('int')) \
                                   .withColumn('Execution_date', col('Execution_date').cast('date'))
            #reorder columns after reading
            album_data = album_data.select('Artist', 'Artist_ID', 'Album_ID', 'Name', 'Type', 'Genres', 
                                        'Label', 'Popularity', 'Available_Markets', 'Release_Date', 
                                        'ReleaseDatePrecision', 'TotalTracks', 'Copyrights', 'Restrictions', 
                                        'External_URL', 'Href', 'Image', 'Uri', 'Execution_date')
            album_data = spark.createDataFrame(album_data.rdd, schema = get_schema('album'))
            print("Finished bronze preprocessing for album data.")
            #upload data into HDFS
            write_HDFS(spark, data = album_data, direct = 'bronze_data/bronze_album', 
                       file_type = 'parquet', mode = "append", partition = 'Execution_date')
        except Exception as e:
            print(f"An error occurred while preprocessing bronze data: {e}")


        """------------------------ BRONZE TRACK -------------------------"""
        track_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'track_collection', 
                                  schema = get_schema('track'))
        track_data = track_data.filter(track_data['Execution_date'] == Execution_date)
        track_data = track_data.withColumn('Execution_date', col('Execution_date').cast('date'))

        #upload data into HDFS
        write_HDFS(spark, data = track_data, direct = 'bronze_data/bronze_track', 
                   file_type = 'parquet', mode = "append", partition = 'Execution_date')


        """------------------------ BRONZE TRACK FEATURE ------------------------"""
        track_feature_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'trackfeature_collection', 
                                          schema = get_schema('trackfeature'))
        track_feature_data = track_feature_data.filter(track_feature_data['Execution_date'] == Execution_date)
        track_feature_data = track_feature_data.withColumn('Execution_date', col('Execution_date').cast('date'))
        
        #upload data into HDFS
        write_HDFS(spark, data = track_feature_data, direct = 'bronze_data/bronze_track_feature', 
                   file_type = 'parquet', mode = "append", partition = 'Execution_date')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Current date argument")
    parser.add_argument('--execution_date', required = False, help = "execution_date")
    args = parser.parse_args()

    print("------------------------------- Bronze task starts! -------------------------------")
    bronze_layer_processing(args.execution_date)
    print("------------------------------ Bronze task finished! -------------------------------")