from spark_schemas import get_schema
from spark_hadoop_operations import get_sparkSession, read_mongoDB, write_HDFS
from pyspark.sql.functions import split, col, get_json_object, array

""" Applying schemas and loading data from MongoDB into HDFS."""
def bronze_task():
    #get spark Session
    with get_sparkSession(appName = 'Bronze_task') as spark:
        """ Artist data. """
        artist_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'artist_collection')
        #drop _id column in mongoDB
        artist_data = artist_data.drop('_id')
        #processing Genres and External_Url columns
        artist_data = artist_data.withColumn('Genres', split(col('Genres'), ",")) \
                                 .withColumn('External_Url', get_json_object(col('External_Url'),'$.spotify'))
        #applying schema
        artist_data = spark.createDataFrame(artist_data.rdd, schema = get_schema('artist'))
        #load data into HDFS
        write_HDFS(spark, artist_data, 'artist', 'parquet')

        """ Album data. """
        album_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'album_collection')
        #drop _id column in mongoDB
        album_data = album_data.drop('_id')
        #applying schema
        album_data = spark.createDataFrame(album_data.rdd, schema = get_schema('album'))
        #load data into HDFS
        write_HDFS(spark, album_data, 'album', 'parquet')

        """ Track data. """
        track_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'track_collection')
        #drop _id
        track_data = track_data.drop('_id')
        #applying schema
        track_data = spark.createDataFrame(track_data.rdd, schema = get_schema('track'))
        #load data into HDFS
        write_HDFS(spark, track_data, 'track', 'parquet')

        """ Track Feature data. """
        track_feature_data = read_mongoDB(spark, database_name = 'music_database', collection_name = 'trackfeature_collection')
        #drop _id
        track_feature_data = track_feature_data.drop('_id')
        #applying schema
        track_feature_data = spark.createDataFrame(track_feature_data.rdd, schema = get_schema('trackfeature'))
        #load data into mongoDB
        write_HDFS(spark, track_feature_data, 'trackfeature', 'parquet')


bronze_task()

        