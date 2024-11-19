from spark_script.sparkIO_operations import *
from spark_schemas import get_schema
from pyspark.sql.functions import split, col, get_json_object, to_date, regexp_replace, length

""" Applying schemas and loading data from MongoDB into HDFS."""
def bronze_task():
    #get spark Session
    with get_sparkSession(appName = 'Bronze_task') as spark:
        """------------------------ BRONZE ARTIST ------------------------"""
        artist_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'artist_collection')
        print("Starting bronze preprocessing for artist data...")
        #preprocessing before loading data
        try:
            artist_data = artist_data.withColumn('Genres', split(col('Genres'), ",")) \
                                    .withColumn('Followers', col('Followers').cast('int')) \
                                    .withColumn('Popularity', col('Popularity').cast('int')) \
                                    .withColumn('External_Url', get_json_object(col('External_Url'),'$.spotify')) \
            #reorder columns after reading 
            artist_data = artist_data.select('Artist_ID', 'Artist_Name', 'Genres', 
                                            'Followers', 'Popularity', 'Artist_Image', 
                                            'Artist_Type', 'External_Url', 'Href', 'Artist_Uri')
            #applying schema        
            artist_data = spark.createDataFrame(artist_data.rdd, schema = get_schema('artist'))

            print("Finished bronze preprocessing for artist data.")

            #upload data into HDFS
            write_HDFS(spark, data = artist_data, direct = 'bronze_data/bronze_artist', file_type = 'parquet')
        except Exception as e:
            print(f"An error occurred while preprocessing bronze data: {e}")


        """------------------------ BRONE ALBUM ------------------------"""
        album_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'album_collection')
        print("Starting bronze preprocessing for album data...")
        try:
            album_data = album_data.withColumn('Popularity', col('Popularity').cast('int')) \
                                .withColumn('Release_Date', to_date('Release_Date', "MM/dd/yyyy")) \
                                .withColumn('TotalTracks', col('TotalTracks').cast('int'))
            #reorder columns after reading
            album_data = album_data.select('Artist', 'Artist_ID', 'Album_ID', 'Name', 'Type', 'Genres', 
                                        'Label', 'Popularity', 'Available_Markets', 'Release_Date', 
                                        'ReleaseDatePrecision', 'TotalTracks', 'Copyrights', 'Restrictions', 
                                        'External_URL', 'Href', 'Image', 'Uri')
            album_data = spark.createDataFrame(album_data.rdd, schema = get_schema('album'))
            print("Finished bronze preprocessing for album data.")
            #upload data into HDFS
            write_HDFS(spark, data = album_data, direct = 'bronze_data/bronze_album', file_type = 'parquet')
        except Exception as e:
            print(f"An error occurred while preprocessing bronze data: {e}")


        """------------------------ BRONZE TRACK -------------------------"""
        track_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'track_collection', 
                                  schema = get_schema('track'))
        #upload data into HDFS
        write_HDFS(spark, data = track_data, direct = 'bronze_data/bronze_track', file_type = 'parquet')


        """------------------------ BRONZE TRACK FEATURE ------------------------"""
        track_feature_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'trackfeature_collection', 
                                          schema = get_schema('trackfeature'))
        #upload data into HDFS
        write_HDFS(spark, data = track_feature_data, direct = 'bronze_data/bronze_track_feature', file_type = 'parquet')


if __name__ == "__main__":
    print("------------------------------- Bronze task starts! -------------------------------")
    bronze_task()
    print("------------------------------ Bronze task finished! -------------------------------")