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
    
    """ Run the Bronze layer task. """
    bronze_layer_task = SparkSubmitOperator(
        task_id = 'bronze_layer_task',
        conn_id = 'spark_default',
        application = '/opt/airflow/dags/spark_script/bronze_script.py',
        name = 'bronze_layer_processing_script',
        conf={
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.executor.instances': '2',
            "spark.sql.shuffle.partitions": '4',
            "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
        }
    )

    """ Run the Silver layer task. """
    silver_layer_task = SparkSubmitOperator(
        task_id = 'silver_layer_task',
        conn_id = 'spark_default',
        application = '/opt/airflow/dags/spark_script/silver_script.py',
        name = 'silver_layer_processing_script',
        conf={
            "spark.executor.memory": '2g',
            "spark.executor.cores": '2',
            "spark.executor.instances": '2',
            "spark.sql.shuffle.partitions": '4',
            "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
        }
    )

    """ Run the Gold layer task. """
    gold_layer_task = SparkSubmitOperator(
        task_id = 'gold_layer_task',
        conn_id = 'spark_default',
        application = '/opt/airflow/dags/spark_script/gold_script.py',
        name = 'silver_layer_processing_script',
        conf={
            "spark.executor.memory": '2g',
            "spark.executor.cores": '2',
            "spark.executor.instances": '2',
            "spark.sql.shuffle.partitions": '4',
            "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
        }
    )
    bronze_layer_task >> silver_layer_task >> gold_layer_task
