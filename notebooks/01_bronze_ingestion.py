# Databricks notebook source
## Bronze Layer: Data Ingestion and Validation
#Extracting raw data in parquet format and quality checks 

# Reading all 12 months from taxi_raw 
path = "/Volumes/workspace/default/taxi_raw/yellow_tripdata_2025-*.parquet"
df = spark.read.parquet(path)

total_rides = df.count()

assert total_rides > 0, "No data!"
print(f"✅ Quality checks gone right! {total_rides:,} rides.")

# Result
display(df.limit(5))

# Creating table Bronze
df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.bronze_yellow_taxi")

print("✅ Bronze layer done! Table bronze_yellow_taxi is ready.")