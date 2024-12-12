from sparkIO_operations import *
from pyspark.sql.functions import monotonically_increasing_id, concat, col, lit
import argparse

""" Gold layer. """
def gold_layer_processing():

    with get_sparkSession('Gold_task_spark') as spark:
        #Read data from HDFS
        silver_artist = read_HDFS(spark, HDFS_dir = 'silver_data/silver_artist', file_type = 'parquet')
        silver_artist.cache()

        silver_album = read_HDFS(spark, HDFS_dir = 'silver_data/silver_album', file_type = 'parquet')
        silver_album.cache()

        silver_track = read_HDFS(spark, HDFS_dir = 'silver_data/silver_track', file_type = 'parquet')
        silver_track.cache()

        silver_track_feature = read_HDFS(spark, HDFS_dir = 'silver_data/silver_track_feature', file_type = 'parquet')
        silver_track_feature.cache()


        """ Create dim_genres table. """
        dim_genres = silver_artist.select('genres').distinct()
        dim_genres = dim_genres.filter(col('genres').isNotNull())
        #add primary key
        dim_genres = dim_genres.withColumn("id", monotonically_increasing_id()) \
                               .withColumn("id", concat(lit("gns"), col('id')))
        #reorder columns
        dim_genres = dim_genres.select("id", "genres")
        #load data into HDFS
        write_HDFS(spark, data = dim_genres, direct = 'gold_data/dim_genres', file_type = 'parquet')


        """ Create dim_artist table. """
        #just drop genres column and distinct row
        dim_artist = silver_artist.drop('genres').distinct()
        write_HDFS(spark, data = dim_artist, direct = 'gold_data/dim_artist', file_type = 'parquet')


        """ Create dim_artist_genres table. """
        #select necessary columns in artist table
        dim_artist_genres = silver_artist.select('id', 'genres') \
                                         .withColumnRenamed('id', 'artist_id')
        #joining tables to map artist IDs and genre IDs
        dim_genres = read_HDFS(spark, HDFS_dir = 'gold_data/dim_genres', file_type = 'parquet')
        dim_artist_genres = dim_artist_genres.join(dim_genres, on = 'genres', how = 'left') \
                                             .withColumnRenamed('id', 'genres_id')
        #drop genres column
        dim_artist_genres = dim_artist_genres.drop('genres')
        #load data into HDFS
        write_HDFS(spark, data = dim_artist_genres, direct = 'gold_data/dim_artist_genres', file_type = 'parquet')


        """ Create dim_album table. """
        #just drop unnecessary columns 
        dim_album = silver_album.drop('artist', 'artist_id', 'total_tracks', 'release_date_precision')
        #load data into HDFS
        write_HDFS(spark, data = dim_album, direct = 'gold_data/dim_album', file_type = 'parquet')


        """ Create dim_track_feature table. """
        #we don't need to do anything since the dim_track_feature table is complete
        #load data into HDFS
        write_HDFS(spark, data = silver_track_feature, direct = 'gold_data/dim_track_feature', file_type = 'parquet')


        """ Create fact_track table. """
        #drop album name and rename track id column
        fact_track = silver_track.drop('album_name') \
                                 .withColumnRenamed('id', 'track_id')
        #get artist ID from silver album table to create a foreign key for the fact_track table
        silver_album = silver_album.select('id', 'artist_id') \
                                   .withColumnRenamed('id', 'album_id')
        fact_track = fact_track.join(silver_album, on = 'album_id', how = 'inner')
        #reorder columns
        fact_track = fact_track.select('track_id', 'artist_id', 'album_id', 'name', 'track_number', 
                                        'disc_number', 'duration_ms', 'explicit', 'url', 'restriction', 'preview')
        #load data into HDFS
        write_HDFS(spark, data = fact_track, direct = 'gold_data/fact_track', file_type = 'parquet')
    
    

if __name__ == "__main__":
    print("------------------------------- Gold task starts! -------------------------------")
    gold_layer_processing()
    print("------------------------------- Gold task finished! -------------------------------")