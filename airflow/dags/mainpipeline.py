from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from mongoDB.get_daily_artist_name import *
from mongoDB.get_daily_artist import *
import sql
import sql.create_db_scm_table

""" Set default args for DAG. """
default_args = {
    'owner': 'huynhthuan',
    'depends_on_past': False,
}

""" Create a DAG to upload initial crawled CSV data into MongoDB."""
with DAG(
    description = "DAG to upload initial crawled CSV data into MongoDB.",
    dag_id = 'Initial_load',
    default_args = default_args,
    schedule_interval = '@once',
    start_date = datetime(2024, 11, 10)
) as init_load_dag:
    
    """ Load initial csv data. """
    initial_load = SparkSubmitOperator(
        task_id = 'initial_load',
        conn_id = 'spark_default',
        application = '/opt/airflow/dags/init_load.py',
        name = 'upload_initial_csv_file',
        packages = 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0'
    )

with DAG(
    description = 'Extract, Transform, and Load Music Data for Analytics and Recommendation System',
    dag_id = 'Music_data_pipeline',
    default_args = default_args,
    schedule_interval = None,
    render_template_as_native_obj = True,
    catchup = False
) as daily_dag:
    Execution_date = "2024-11-23"

    # load_artist_name_mongo_task = PythonOperator(
    #     task_id = "load_artist_name_mongo_task",
    #     python_callable = load_daily_artist_name_mongoDB,
    #     op_kwargs = {'Execution_date': Execution_date}
    # )

    load_artist_mongo_task = PythonOperator(
        task_id = "load_artist_mongo_task",
        python_callable = load_daily_artist_mongoDB,
        op_kwargs = {'Execution_date': Execution_date}
    )

    # with TaskGroup(group_id = 'ETL_HDFS_task') as tg_etl_hdfs_taskgroup:
    #     """ Run the Bronze layer task. """
    #     bronze_layer_task = SparkSubmitOperator(
    #         task_id = 'bronze_layer_task',
    #         conn_id = 'spark_default',
    #         application = '/opt/airflow/dags/spark_script/bronze_script.py',
    #         name = 'bronze_layer_processing_script',
    #         conf={
    #             'spark.executor.memory': '2g',
    #             'spark.executor.cores': '2',
    #             'spark.executor.instances': '2',
    #             "spark.sql.shuffle.partitions": '4',
    #             "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
    #         }
    #     )

    #     """ Run the Silver layer task. """
    #     silver_layer_task = SparkSubmitOperator(
    #         task_id = 'silver_layer_task',
    #         conn_id = 'spark_default',
    #         application = '/opt/airflow/dags/spark_script/silver_script.py',
    #         name = 'silver_layer_processing_script',
    #         conf={
    #             "spark.executor.memory": '2g',
    #             "spark.executor.cores": '2',
    #             "spark.executor.instances": '2',
    #             "spark.sql.shuffle.partitions": '4',
    #             "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
    #         }
    #     )

    #     """ Run the Gold layer task. """
    #     gold_layer_task = SparkSubmitOperator(
    #         task_id = 'gold_layer_task',
    #         conn_id = 'spark_default',
    #         application = '/opt/airflow/dags/spark_script/gold_script.py',
    #         name = 'silver_layer_processing_script',
    #         conf={
    #             "spark.executor.memory": '2g',
    #             "spark.executor.cores": '2',
    #             "spark.executor.instances": '2',
    #             "spark.sql.shuffle.partitions": '4',
    #             "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
    #         }
    #     )
    #     bronze_layer_task >> silver_layer_task >> gold_layer_task


    # """ SQL TASK. """
    # #database name
    # SNOWFLAKE_DATABASE = "SPOTIFY_MUSIC_DB"

    # #schema name
    # SNOWFLAKE_SCHEMA = SNOWFLAKE_DATABASE + ".SPOTIFY_MUSIC_SCHEMA"

    # #list of table name
    # SNOWFLAKE_DIM_GENRES = SNOWFLAKE_SCHEMA + ".dim_genres"

    # SNOWFLAKE_DIM_ARTIST = SNOWFLAKE_SCHEMA + ".dim_artist"

    # SNOWFLAKE_DIM_ARTIST_GENRES = SNOWFLAKE_SCHEMA + ".dim_artist_genres"

    # SNOWFLAKE_DIM_ALBUM = SNOWFLAKE_SCHEMA + ".dim_album"

    # SNOWFLAKE_DIM_TRACK_FEATURE = SNOWFLAKE_SCHEMA + ".dim_track_feature"

    # SNOWFLAKE_FACT_TRACK = SNOWFLAKE_SCHEMA + ".fact_track"

    # """ Set task group for creating database, schema and table task."""
    # with TaskGroup(group_id = 'create_database_schema_and_tables') as tg_snowflake_create_table_taskgroup:
    #     #create database
    #     create_snowflake_db_task = SnowflakeOperator(
    #         task_id = 'create_snowflake_db_task',
    #         snowflake_conn_id = 'snowflake_default',
    #         sql = sql.create_db_scm_table.create_spotify_music_db,
    #         params = {'database_name': SNOWFLAKE_DATABASE}
    #     )

    #     #create schema
    #     create_snowflake_schema_task = SnowflakeOperator(
    #         task_id = 'create_snowflake_schema_task',
    #         snowflake_conn_id = 'snowflake_default',
    #         sql = sql.create_db_scm_table.create_spotify_music_schema,
    #         params = {'schema_name': SNOWFLAKE_SCHEMA}
    #     )

    #     #create dim_genres
    #     create_snowflake_dim_genres_task = SnowflakeOperator(
    #         task_id = 'create_snowflake_dim_genres_task',
    #         snowflake_conn_id = 'snowflake_default',
    #         sql = sql.create_db_scm_table.create_dim_genres,
    #         params = {'table_name': SNOWFLAKE_DIM_GENRES}
    #     )

    #     #create dim_artist
    #     create_snowflake_dim_artist_task = SnowflakeOperator(
    #         task_id = 'create_snowflake_dim_artist_task',
    #         snowflake_conn_id = 'snowflake_default',
    #         sql = sql.create_db_scm_table.create_dim_artist,
    #         params = {'table_name': SNOWFLAKE_DIM_ARTIST}
    #     )

    #     #create dim_artist_genres
    #     create_snowflake_dim_artist_genres_task = SnowflakeOperator(
    #         task_id = 'create_snowflake_dim_artist_genres_task',
    #         snowflake_conn_id = 'snowflake_default',
    #         sql = sql.create_db_scm_table.create_dim_artist_genres,
    #         params = {'table_name': SNOWFLAKE_DIM_ARTIST_GENRES}
    #     )

    #     #create dim_album
    #     create_snowflake_dim_album_task = SnowflakeOperator(
    #         task_id = 'create_snowflake_dim_album_task',
    #         snowflake_conn_id = 'snowflake_default',
    #         sql = sql.create_db_scm_table.create_dim_album,
    #         params = {'table_name': SNOWFLAKE_DIM_ALBUM}
    #     )

    #     #create dim_track_feature
    #     create_snowflake_dim_track_feature_task = SnowflakeOperator(
    #         task_id = 'create_snowflake_dim_track_feature_task',
    #         snowflake_conn_id = 'snowflake_default',
    #         sql = sql.create_db_scm_table.create_dim_track_feature,
    #         params = {'table_name': SNOWFLAKE_DIM_TRACK_FEATURE}
    #     )

    #     #create fact_track
    #     create_snowflake_fact_track_task = SnowflakeOperator(
    #         task_id = 'create_snowflake_fact_track_task',
    #         snowflake_conn_id = 'snowflake_default',
    #         sql = sql.create_db_scm_table.create_fact_track,
    #         params = {'table_name': SNOWFLAKE_FACT_TRACK}
    #     )

    #     #set pipelines
    #     create_snowflake_db_task >> create_snowflake_schema_task \
    #     >> [create_snowflake_dim_genres_task, create_snowflake_dim_artist_task, \
    #         create_snowflake_dim_artist_genres_task, create_snowflake_dim_album_task, \
    #         create_snowflake_dim_track_feature_task, create_snowflake_fact_track_task]

    #gold_layer_task >> create_snowflake_db_task