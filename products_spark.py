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
product_data.show(10)

# transformations DATAFRAME API

output = product_data.select("product_name", "quantity", "unit_price")\
        .filter((product_data["quantity"] > 0) & (product_data["unit_price"] > 0))\
        .withColumn("total_price", func.col("quantity") * func.col("unit_price"))\
        .orderBy(func.col("total_price").desc())\
        .cache()


output.printSchema()

# create temporary view
output.createOrReplaceTempView("products_view")
# run SQL query, SQL API
# query = """
#   SELECT product_name, quantity, unit_price, total_price
#   FROM products_view
#   ORDER BY total_price DESC
# """
# result = spark.sql(query)
# result.show(10)

# save the output to a new CSV file
output.write.format("csv")\
   .mode("overwrite")\
   .option("header", "true")\
   .option("path", "data/products_output.csv")\
   .partitionBy("product_name")\
   .save()