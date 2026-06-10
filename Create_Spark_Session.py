from pyspark.sql import SparkSession

spark=(
    SparkSession.builder \
    .appName("Ecomerce")
    .master("local[*]")
    .getOrCreate()
)
print("Spark Session Created")

