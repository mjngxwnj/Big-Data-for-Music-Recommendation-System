import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pymongo import MongoClient
from pyspark import SparkConf
from contextlib import contextmanager
import pyspark.sql

""" Context manager for creating Spark Session. """
@contextmanager
def get_sparkSession(appName: str, master: str = 'local'):
    #declare sparkconf
    conf = SparkConf()

    #set config
    conf = conf.setAppName(appName) \
               .setMaster(master) \
               .set("spark.executor.memory", "4g") \
               .set("spark.executor.cores", "2") \
               .set("spark.sql.shuffle.partitions", "4") \
               .set("spark.sql.legacy.timeParserPolicy", "LEGACY") \
               .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
               .set("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4")
    
    #create Spark Session
    spark = SparkSession.builder.config(conf = conf).getOrCreate()

    print(f"Successfully created Spark Session with app name: {appName} and master: {master}!")

    #yield spark
    try:
        yield spark

    finally:
        #must stop Spark Session
        spark.stop()
        print("Successfully stopped Spark Session!")


""" Read data from mongoDB. """
def read_mongoDB(spark: SparkSession, database_name: str, collection_name: str, query: dict = None,
                 schema: StructType = None, username: str = 'huynhthuan', password: str = 'password', 
                 host: str = 'mongo', port: str = 27017) -> pyspark.sql.DataFrame:
    
    #check params
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a SparkSession!")
    
    if query is not None and not isinstance(query, dict):
        raise TypeError("query must be a dict!")
    
    if schema is not None and not isinstance(schema, StructType):
        raise TypeError("schema must be a StructType!")
    
    #uri mongoDB 
    uri = f"mongodb://{username}:{password}@{host}:{port}/{database_name}.{collection_name}?authSource=admin"

    print(f"Starting to read data from database '{database_name}' and collection '{collection_name}'...")
  
    #read data
    try:
        data = spark.read.format('mongodb') \
                         .option("spark.mongodb.read.connection.uri", uri) \
                         .option('header', 'true')
        
        data = data.schema(schema).load() if schema is not None else data.load()

        return data 
    
    except Exception as e:
        print(f"An error occurred while reading data from mongoDB: {e}")


""" Read data from HDFS. """
def read_HDFS(spark: SparkSession, HDFS_dir: str, file_type: str) -> pyspark.sql.DataFrame:
    #check params
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a SparkSession!")
    
    #set HDFS path
    HDFS_path = f"hdfs://namenode:9000/datalake/{HDFS_dir}"

    print(f"Starting to read data from {HDFS_path}...")

    #read data
    try:
        data = spark.read.format(file_type).option('header', 'true').load(HDFS_path)
        #return data
        return data
    
    except Exception as e:
        print(f"An error occurred while reading data from HDFS: {e}")


""" Write data into HDFS. """
def write_HDFS(spark: SparkSession, data: pyspark.sql.DataFrame, direct: str, 
               file_type: str, mode: str = 'overwrite', partition: str = None):
    #check params
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a SparkSession!")
    
    if not isinstance(data, pyspark.sql.DataFrame):
        raise TypeError("data must be a DataFrame!")

    #set HDFS path  
    HDFS_path = f"hdfs://namenode:9000/datalake/{direct}"
    table_name = direct.split('/')[-1]

    print(f"Starting to upload '{table_name}' into {HDFS_path}...")
    
    #write data
    try:
        if partition is not None:
            data.write.format(file_type) \
                      .option('header', 'true') \
                      .mode(mode) \
                      .partitionBy('Execution_date') \
                      .save(HDFS_path)
        else:
            data.write.format(file_type) \
                      .option('header', 'true') \
                      .mode(mode) \
                      .save(HDFS_path)
        
        print(f"Successfully uploaded '{table_name}' into HDFS.")

    except Exception as e:
        print(f"An error occurred while upload data into HDFS: {e}")

""" Write data into SnowFlake Data Warehouse. """
def write_SnowFlake(spark: SparkSession, data: pyspark.sql.DataFrame, table_name: str):
    #check params
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a SparkSession!")
    
    if not isinstance(data, pyspark.sql.DataFrame):
        raise TypeError("data must be a DataFrame!")
    
    snowflake_connection_options = {
        "sfURL": "https://sl70006.southeast-asia.azure.snowflakecomputing.com",
        "sfUser": "HUYNHTHUAN", 
        "sfPassword": "Thuan123456",
        "sfWarehouse": "COMPUTE_WH",
        "sfDatabase": "SPOTIFY_MUSIC_DB" 
    }

    print(f"Starting to upload {table_name.split('.')[-1]} into SnowFlake...")
    try:
        data.write.format("snowflake") \
                .options(**snowflake_connection_options) \
                .option("dbtable", table_name) \
                .mode('overwrite') \
                .save()
        print(f"Successfully uploaded '{table_name}' into SnowFlake.")
    except Exception as e:
        print(f"An error occurred while upload data into HDFS: {e}")
    