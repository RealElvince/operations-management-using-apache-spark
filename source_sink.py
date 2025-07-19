from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# define input,output and checkpoint directories
input_dir = "data/input"
output_dir = "data/output"
checkpoint_dir = "./checkpoint"

# create Spark session
spark = SparkSession.builder \
    .appName("source_sink")\
    .getOrCreate()



# define input file schema
input_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("friends", IntegerType(), True)
    ])


# input data src
inputDF =(
    spark.readStream\
    .format("csv")\
    .schema(input_schema)\
    .option("header", "true")\
    .option("path", input_dir)\
    .load()
)

# transform data
outputDF = inputDF.select("id","name","friends").where(inputDF["age"] > 30)