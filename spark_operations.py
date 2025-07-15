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

# transformations DATAFRAME API
data_2 = data.select("industry","value")\
    .filter((data["value"] > 200) & (data["industry"] != "total"))\
    .orderBy((data["value"].desc()))


data_2.printSchema()
data_2.show(10)


# create temporary view
data_2.createOrReplaceTempView("operations_view")

# run SQL query,SQL API
threshold = 200
query = f"""
  SELECT industry, value
  FROM operations_view
  WHERE value > {threshold} AND industry != 'total'
  ORDER BY value DESC
"""
result = spark.sql(query)
result.show(10)

# create a global temporary view

data.createGlobalTempView("global_operations_view")
# run SQL query on global temporary view
global_query =  """
  SELECT *
  FROM global_operations_view"""
global_result = spark.sql(global_query)
global_result.show(10)

# list all database tables
tables = spark.catalog.listTables()
for table in tables:
    print(f"Table Name: {table.name}, Is Temporary: {table.isTemporary}")

# list all databases
databases = spark.catalog.listDatabases()
for db in databases:
    print(f"Database Name: {db.name}, Description: {db.description}")