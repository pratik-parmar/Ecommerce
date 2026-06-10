"""
File Name: 02_ingest_raw_data.py

Purpose:
1. Download Olist E-commerce Dataset from Kaggle
2. Read CSV files into PySpark DataFrames
3. Store them as Bronze Layer Parquet files

"""

# Import Libraries

import os
import kagglehub

from pyspark.sql import SparkSession


# Create Spark Session


spark = (
    SparkSession.builder
    .appName("Olist Data Ingestion")
    .master("local[*]")
    .getOrCreate()
)


# Download Dataset


print("Downloading Dataset...")

path = kagglehub.dataset_download(
    "olistbr/brazilian-ecommerce"
)

print(f"\nDataset Location:\n{path}")


# List Files


print("\nAvailable Files:\n")

for file in os.listdir(path):
    print(file)


# Read Customers Dataset


customers_df = spark.read.csv(
    os.path.join(path,
                 "olist_customers_dataset.csv"),
    header=True,
    inferSchema=True
)


# Read Orders Dataset


orders_df = spark.read.csv(
    os.path.join(path,
                 "olist_orders_dataset.csv"),
    header=True,
    inferSchema=True
)


# Read Order Items Dataset


order_items_df = spark.read.csv(
    os.path.join(path,
                 "olist_order_items_dataset.csv"),
    header=True,
    inferSchema=True
)

# ==========================
# Read Payments Dataset
# ==========================

payments_df = spark.read.csv(
    os.path.join(path,
                 "olist_order_payments_dataset.csv"),
    header=True,
    inferSchema=True
)


# Read Reviews Dataset


reviews_df = spark.read.csv(
    os.path.join(path,
                 "olist_order_reviews_dataset.csv"),
    header=True,
    inferSchema=True
)


# Read Products Dataset


products_df = spark.read.csv(
    os.path.join(path,
                 "olist_products_dataset.csv"),
    header=True,
    inferSchema=True
)


# Verify Data


print("\nCustomers Count:")
print(customers_df.count())

print("\nOrders Count:")
print(orders_df.count())

print("\nPayments Count:")
print(payments_df.count())


# Print Schema


print("\nCustomers Schema:")
customers_df.printSchema()


# Save Bronze Layer

customers_df.write.mode("overwrite").parquet(
    "output/bronze/customers"
)

orders_df.write.mode("overwrite").parquet(
    "output/bronze/orders"
)

order_items_df.write.mode("overwrite").parquet(
    "output/bronze/order_items"
)

payments_df.write.mode("overwrite").parquet(
    "output/bronze/payments"
)

reviews_df.write.mode("overwrite").parquet(
    "output/bronze/reviews"
)

products_df.write.mode("overwrite").parquet(
    "output/bronze/products"
)

print("\nBronze Layer Created Successfully")