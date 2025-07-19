from pyspark.sql  import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .appName("spark_streaming")\
    .getOrCreate()


# define input src

lines = (
    spark.readStream\
    .format("socket")\
    .option("host", "localhost")\
    .option("port", 9999)
    .load()
)

# transform data
words = lines.select(split(col("value"), "\\s").alias("word"))\

# Get the word count
counts = words.groupBy("word").count()

# Define the checkpoint directory
checkpoint_dir = "./checkpoint"

# start streaming define the necessary configuration
streaing_query =(
    counts.writeStream\
    .outputMode("complete")\
    .format("console")\
    .option("checkpointLocation", checkpoint_dir)\
    .start()
)

# wait for the termination of the query     
streaing_query.awaitTermination()