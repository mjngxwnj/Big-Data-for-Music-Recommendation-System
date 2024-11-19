from sparkIO_operations import *


def load_data_Snowflake(spark):
    dim_artist = read_HDFS(spark, HDFS_dir = 'gold_data/dim_artist', file_type = 'parquet')
    