from streamlit_frontent import *
from streamlit_backend import *
from pyspark.sql import SparkSession
from pyspark import SparkConf

app = Streamlit_UI()
app.generate_application()