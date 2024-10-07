from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import mongoDB
import mongoDB.load_data_mongoDb

""" Set default args for DAG. """
default_args = {
    'owner': 'huynhthuan',
    'depends_on_past': False,
    'start_date': datetime(2024,9,28)
}

""" Create dag. """
with DAG(
    description = 'Extract, Transform, and Load Music Data for Analytics and Recommendation System',
    dag_id = 'Music_data_pipeline',
    default_args = default_args,
    schedule_interval = None,
    render_template_as_native_obj = True,
    catchup = False
) as dag:
    
    """ Load artist data into mongoDB. """
    load_artist_mongoDB = PythonOperator(
        task_id = 'load_artist_mongoDB',
        python_callable = mongoDB.load_data_mongoDb.load_mongodb_artist
    )


    # demo_pipeline2 = SparkSubmitOperator(
    #     task_id = 'demo_pipeline2',
    #     application = '/opt/airflow/dags/spark_script/data_read_mongoDB.py',
    #     conn_id = 'spark_default',
    #     packages = 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0',
    # )

load_artist_mongoDB