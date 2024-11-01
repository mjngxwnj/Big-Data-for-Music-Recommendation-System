import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pymongo import MongoClient
from pyspark import SparkConf
from contextlib import contextmanager
import pyspark.sql

""" Context manager for creating Spark Session. """
@contextmanager
def get_sparkSession(appName: str, master: str = 'local[4]'):
    #declare sparkconf
    conf = SparkConf()

    #set config
    conf = conf.setAppName(appName) \
               .setMaster(master) \
               .set("spark.executor.memory", "4g") \
               .set("spark.executor.cores", "4") \
               .set("spark.sql.legacy.timeParserPolicy", "LEGACY") \
               .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
    
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
def read_mongoDB(spark: SparkSession, database_name: str, collection_name: str, schema: StructType = None,
                 username: str = 'huynhthuan', password: str = 'password', 
                 host: str = 'mongo', port: str = 27017) -> pyspark.sql.DataFrame:
    
    #check params
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a SparkSession!")
    
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
    
    except Exception:
        print("An error occurred while reading data from mongoDB.")


""" Read data from HDFS. """
def read_HDFS(spark: SparkSession, HDFS_dir: str, file_type: str) -> pyspark.sql.DataFrame:
    #check params
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a SparkSession!")
    
    #set HDFS path
    HDFS_path = f"hdfs://namenode:9000/{HDFS_dir}"

    print(f"Starting to read data from {HDFS_path}...")

    #read data
    try:
        data = spark.read.format(file_type).option('header', 'true').load(HDFS_path)
        #return data
        return data
    
    except Exception:
        print("An error occurred while reading data from HDFS.")


""" Write data into HDFS. """
def write_HDFS(spark: SparkSession, data: pyspark.sql.DataFrame, table_name: str, file_type: str):
    #check params
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a SparkSession!")
    
    if not isinstance(data, pyspark.sql.DataFrame):
        raise TypeError("data must be a DataFrame!")

    #set HDFS path  
    HDFS_path = f"hdfs://namenode:9000/datalake/{table_name}"

    print(f"Starting to upload '{table_name}' into {HDFS_path}...")
    
    #write data
    try:
        data.write.format(file_type) \
                  .option('header', 'true') \
                  .mode('append') \
                  .save(HDFS_path)
        
        print(f"Successfully uploaded '{table_name}' into HDFS.")

    except Exception:
        print("An error occurred while upload data into HDFS!")