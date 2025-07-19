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
    .load(input_dir)
)

# transform data
outputDF = inputDF.select("id","name","friends").where(inputDF["age"] > 30)


# streaming query
streaming_query = (
    outputDF.writeStream\
    .outputMode("append")\
    .format("csv")\
    .option("checkpointLocation", checkpoint_dir)\
    .option("path", output_dir)\
    .start())

# wait for the termination of the query
streaming_query.awaitTermination()