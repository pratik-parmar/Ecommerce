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


# checking for NUll values 

null_customer_ids=customer_df.filter(col("customer_id").isNull()).count() #provide the count of null values present in cutomer_id column provide count number of null values present 
print(null_customer_ids)
null_unique_id=customer_df.filter(col("customer_unique_id").isNull()).count()
print(null_unique_id)
null_city=customer_df.filter(col("customer_city").isNull())
null_city.show()

null_customer_zip_code=customer_df.filter(col("customer_zip_code_prefix").isNull()).count()
print(null_customer_zip_code)


# remove Duplicate Records

customers_df=customer_df.drop_duplicates() # if duplicate records exists will drop them 
print("\n Row count after removing duplicates")
print(customers_df.count())

# looking for invalid customer id present in the dataset or not
invalid_customers = customers_df.filter(col("customer_id").isNull())
invalid_count = invalid_customers.count()
print(f"Total invalid rows found: {invalid_count}")


# removing invalid cutomer id
customers_df=customers_df.filter(col("customer_id").isNotNull())
# filling customer city values
customers_df=customer_df.na.fill({"customer_city":"UNKNOWN"})
 #Fill Missing State Values
customers_df=customer_df.na.fill({"customer_state":"UNKNOWN"})
# Verify Clean Data

print("\nCleaned Data Sample:")

customers_df.show(5,truncate=False)
# Final Record Count
print("\nFinal Row Count:")
print(customers_df.count())


#saving the new  data

customers_df.write.mode(
    "overwrite"
).parquet(
    "output/silver/customers"
)