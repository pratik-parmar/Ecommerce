from pyspark.sql import SparkSession

print("Starting test file...")

spark = (
    SparkSession.builder
    .appName("Test Orders")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Created")

df = spark.read.parquet(
    "output/silver/orders"
)

print("Data Loaded")

print("Total Records:", df.count())

df.show(15)

print("Finished")