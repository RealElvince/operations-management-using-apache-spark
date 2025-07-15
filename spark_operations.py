from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("operations")\
    .getOrCreate()


data = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .option("path","data/operations.csv")\
    .load()

data.printSchema()
