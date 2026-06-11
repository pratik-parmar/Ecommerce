from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark =(
    SparkSession.builder
    .appName("Customer File Cleaning")
    .master("local[*]")
    .getOrCreate()
)
#Read bronze layer

customer_df=spark.read.parquet("output/bronze/customers") # Parquet and ORC formats are highly optimized since they store their own cryptographic metadata and schemas natively

# initial data inspection

print("\n original raw count")
print(customer_df.count())

print("/n Schema :")
customer_df.printSchema() # schema provide column name Data type of that particular column like string interger etc and provide nullability nullable a boolena flag true or false indication weather column is allowed to contain missing or null values.

print("\nSample Data:")
customer_df.show(5, truncate=False) #it will display  exactly first  5 rows it limits and truncate =false means it make sure the text is fully displayed rather than being cutoff with dots.


