from pyspark.sql  import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .appName("spark_streaming")\
    .getOrCreate()


lines = (
    spark.readStream\
    .format("socket")\
    .option("host", "localhost")\
    .option("port", 9999)
    .load()\
    .cache()
)
             