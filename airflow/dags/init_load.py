from spark_script.spark_hadoop_operations import get_sparkSession
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
        df_ArtistName = spark.read.option('header', 'true').csv("/opt/data/ArtistName.csv")
        df_Artist = spark.read.option('header', 'true').csv("/opt/data/Artist.csv")
        df_Album = spark.read.option('header', 'true').csv("/opt/data/Album.csv")
        df_Track = spark.read.option('header', 'true').csv("/opt/data/Track.csv")
        df_TrackFeature = spark.read.option('header', 'true').csv("/opt/data/TrackFeature.csv")


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