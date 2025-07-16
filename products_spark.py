from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, FloatType
import pyspark.sql.functions  as func

spark = SparkSession.builder \
    .appName("products_spark")\
    .getOrCreate()

product_schema = StructType([
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", FloatType(), True)
])


product_data = spark.read.format("csv")\
    .schema(product_schema)\
    .option("inferSchema", "false")\
    .option("path", "data/products.csv")\
    .load()

product_data.printSchema()