# Databricks notebook source
from pyspark.sql import functions as F

df_silver = spark.table("workspace.default.silver_yellow_taxi")

gold_daily_revenue = df_silver.groupBy(F.to_date("tpep_pickup_datetime").alias("date")) \
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"), 
        F.round(F.sum("tip_amount"), 2).alias("total_tips"),     
        F.count("*").alias("trip_count")
    ).orderBy("date")

spark.sql("DROP TABLE IF EXISTS workspace.default.gold_daily_revenue")
gold_daily_revenue.write.mode("overwrite").saveAsTable("workspace.default.gold_daily_revenue")

gold_top_routes = df_silver.groupBy("PULocationID", "DOLocationID") \
    .agg(F.count("*").alias("trip_count")) \
    .orderBy(F.desc("trip_count")) \
    .limit(10)

spark.sql("DROP TABLE IF EXISTS workspace.default.gold_top_routes")
gold_top_routes.write.mode("overwrite").saveAsTable("workspace.default.gold_top_routes")

gold_hourly_activity = df_silver.withColumn("hour", F.hour("tpep_pickup_datetime")) \
    .groupBy("hour") \
    .agg(F.count("*").alias("trip_count")) \
    .orderBy("hour")

spark.sql("DROP TABLE IF EXISTS workspace.default.gold_hourly_activity")
gold_hourly_activity.write.mode("overwrite").saveAsTable("workspace.default.gold_hourly_activity")

gold_hourly_activity.show(24)
gold_daily_revenue.show(50)
gold_top_routes.show(10)
