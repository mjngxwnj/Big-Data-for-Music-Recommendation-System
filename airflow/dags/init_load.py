from spark_script.sparkIO_operations import get_sparkSession
from datetime import datetime
from pyspark.sql.functions import lit
import argparse

""" Load all csv files into mongoDB."""
def initial_load(Execution_date: str):
    with get_sparkSession(appName = "init_load") as spark:
        #uri
        uri_artist_name = "mongodb://huynhthuan:password@mongo:27017/music_database.artist_name_collection?authSource=admin"
        uri_artist = "mongodb://huynhthuan:password@mongo:27017/music_database.artist_collection?authSource=admin"
        uri_album = "mongodb://huynhthuan:password@mongo:27017/music_database.album_collection?authSource=admin"
        uri_track = "mongodb://huynhthuan:password@mongo:27017/music_database.track_collection?authSource=admin"
        uri_trackfeature = "mongodb://huynhthuan:password@mongo:27017/music_database.trackfeature_collection?authSource=admin"

        # read
        df_ArtistName = spark.read.option('header', 'true').csv("/opt/data/ArtistName.csv")
        df_ArtistName = df_ArtistName.withColumn('Execution_date', lit(Execution_date))
        
        df_Artist = spark.read.option('header', 'true').csv("/opt/data/Artist.csv")
        df_Artist = df_Artist.withColumn('Execution_date', lit(Execution_date))

        df_Album = spark.read.option('header', 'true').csv("/opt/data/Album.csv")
        df_Album = df_Album.withColumn('Execution_date', lit(Execution_date))

        df_Track = spark.read.option('header', 'true').csv("/opt/data/Track.csv")
        df_Track = df_Track.withColumn('Execution_date', lit(Execution_date))
        
        df_TrackFeature = spark.read.option('header', 'true').csv("/opt/data/TrackFeature.csv")
        df_TrackFeature = df_TrackFeature.withColumn('Execution_date', lit(Execution_date))

        #write
        try:
            print("Starting load csv files into MongoDB...")
            df_ArtistName.write.format('mongoDB') \
                            .option("spark.mongodb.write.connection.uri", uri_artist_name) \
                            .mode("overwrite") \
                            .save()
            
            df_Artist.write.format('mongoDB') \
                        .option("spark.mongodb.write.connection.uri", uri_artist) \
                        .mode("overwrite") \
                        .save()
            
            df_Album.write.format('mongoDB') \
                        .option("spark.mongodb.write.connection.uri", uri_album) \
                        .mode("overwrite") \
                        .save()
            
            df_Track.write.format('mongoDB') \
                        .option("spark.mongodb.write.connection.uri", uri_track) \
                        .mode("overwrite") \
                        .save()
            
            df_TrackFeature.write.format('mongoDB') \
                                .option("spark.mongodb.write.connection.uri", uri_trackfeature) \
                                .mode("overwrite") \
                                .save()
            print("Successfully uploaded data into mongoDB.")
        except Exception as e:
            print(f"An error occured while loading data: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Current date argument")
    parser.add_argument('--execution_date', required = False, help = "execution_date")
    args = parser.parse_args()

    print("------------------------------- Initial load task starts! -------------------------------")
    initial_load(args.execution_date)
    print("------------------------------- Initial load task finished! -------------------------------")