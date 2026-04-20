# Databricks notebook source
# Databricks notebook source
import sys
import os
sys.path.append(os.path.abspath('..'))

from src.transformations import clean_taxi_data
from pyspark.sql import functions as F

# 1. Extracting bronze
df_bronze = spark.table("workspace.default.bronze_yellow_taxi")

# 2. Transforming data
# Logic moved to src/transformations.py to follow professional DE standards
df_silver = clean_taxi_data(df_bronze)

# Adding partition column
df_silver = df_silver.withColumn("pickup_month", F.month("tpep_pickup_datetime"))

# Saving as Silver table 
spark.sql("DROP TABLE IF EXISTS workspace.default.silver_yellow_taxi")

# Using partitionBy 
df_silver.write \
    .format("delta") \
    .partitionBy("pickup_month") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_yellow_taxi")

print(f"✅ Silver Layer ready! Row count: {df_silver.count():,}")
