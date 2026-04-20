# Databricks notebook source
from pyspark.sql import functions as F

# extracting bronze
df_bronze = spark.table("workspace.default.bronze_yellow_taxi")

# Transforming data
df_silver = df_bronze \
    .filter(
        (F.year(F.col("tpep_pickup_datetime")) == 2025) & 
        (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) &
        (F.col("trip_distance") > 0) & (F.col("trip_distance") < 500) &
        (F.abs(F.col("total_amount")) < 5000)
    ) \
    .withColumn("fare_amount", F.round(F.when(F.abs(F.col("fare_amount")) == 0, F.lit(None)).otherwise(F.abs(F.col("fare_amount"))), 3)) \
    .withColumn("extra", F.round(F.abs(F.col("extra")), 3)) \
    .withColumn("mta_tax", F.round(F.abs(F.col("mta_tax")), 3)) \
    .withColumn("tip_amount", F.round(F.abs(F.col("tip_amount")), 3)) \
    .withColumn("tolls_amount", F.round(F.abs(F.col("tolls_amount")), 3)) \
    .withColumn("total_amount", F.round(F.abs(F.col("total_amount")), 3)) \
    .withColumn("passenger_count", F.when(F.col("passenger_count") == 0, F.lit(None)).otherwise(F.col("passenger_count"))) \
    .withColumn("RatecodeID", F.when(F.col("RatecodeID").between(1, 6), F.col("RatecodeID")).otherwise(F.lit(None))) \
    .withColumn("payment_type", F.when(F.col("payment_type").between(1, 6), F.col("payment_type")).otherwise(F.lit(None))) \
    .withColumn("PULocationID", F.when(F.col("PULocationID") > 0, F.col("PULocationID")).otherwise(F.lit(264))) \
    .withColumn("DOLocationID", F.when(F.col("DOLocationID") > 0, F.col("DOLocationID")).otherwise(F.lit(264))) \
    .withColumn("trip_duration_minutes", F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60, 3)) \
    .filter("trip_duration_minutes < 1440") \
    .dropDuplicates()

# Adding column with ride duration in min
df_silver = df_silver.withColumn(
    "trip_duration_minutes", 
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
)

# Saving as Silver table
spark.sql("DROP TABLE IF EXISTS workspace.default.silver_yellow_taxi")
df_silver.write.format("delta").mode("overwrite").saveAsTable("workspace.default.silver_yellow_taxi")

print(f"✅ Silver Layer ready! Good quality raws left: {df_silver.count():,}")
