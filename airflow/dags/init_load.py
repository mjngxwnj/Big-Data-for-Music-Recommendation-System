from spark_script.sparkIO_operations import *
from pyspark.sql.functions import current_date
""" Load all csv files into mongoDB."""
if __name__ == "__main__":
    with get_sparkSession(appName = "init_load") as spark:
        #uri
        uri_artist_name = "mongodb://huynhthuan:password@mongo:27017/music_database.artist_name_collection?authSource=admin"
        uri_artist = "mongodb://huynhthuan:password@mongo:27017/music_database.artist_collection?authSource=admin"
        uri_album = "mongodb://huynhthuan:password@mongo:27017/music_database.album_collection?authSource=admin"
        uri_track = "mongodb://huynhthuan:password@mongo:27017/music_database.track_collection?authSource=admin"
        uri_trackfeature = "mongodb://huynhthuan:password@mongo:27017/music_database.trackfeature_collection?authSource=admin"

        # read
        print("Starting to load all csv files into MongoDB...")
        df_ArtistName = spark.read.option('header', 'true').csv("/opt/data/ArtistName.csv")
        df_ArtistName = df_ArtistName.withColumn('Execution_date', current_date())
        
        df_Artist = spark.read.option('header', 'true').csv("/opt/data/Artist.csv")
        df_Artist = df_Artist.withColumn('Execution_date', current_date())

        df_Album = spark.read.option('header', 'true').csv("/opt/data/Album.csv")
        df_Album = df_Album.withColumn('Execution_date', current_date())

        df_Track = spark.read.option('header', 'true').csv("/opt/data/Track.csv")
        df_Track = df_Track.withColumn('Execution_date', current_date())
        
        df_TrackFeature = spark.read.option('header', 'true').csv("/opt/data/TrackFeature.csv")
        df_TrackFeature = df_TrackFeature.withColumn('Execution_date', current_date())


        #write
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