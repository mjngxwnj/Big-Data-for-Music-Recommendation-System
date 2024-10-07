from spark_schemas import get_schema
from spark_hadoop_operations import get_sparkSession, read_mongoDB, write_HDFS
from pyspark.sql.functions import split, col, get_json_object, array

""" Applying schemas and loading data from MongoDB into HDFS."""
def bronze_task():
    #get spark Session
    with get_sparkSession(appName = 'Bronze_task') as spark:
        """ Artist table. """
        artist_data = read_mongoDB(spark, database_name = 'artist_database', collection_name = 'artist_collection')
        
        #drop _id column in mongoDB
        artist_data = artist_data.drop('_id')

        #processing Genres and External_Url columns
        artist_data = artist_data.withColumn('Genres', split(col('Genres'), ",")) \
                                 .withColumn('External_Url', get_json_object(col('External_Url'),'$.spotify'))
        
        #applying schema
        artist_data = spark.createDataFrame(artist_data.rdd, schema = get_schema('artist'))
        
        #load data into HDFS
        write_HDFS(spark, artist_data, 'artist', 'parquet')

bronze_task()

        