# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Reading Silver Data
df_silver = spark.table("workspace.default.silver_yellow_taxi")

# Creating a window specification for a cumulative total
window_spec = Window.partitionBy(F.lit(1)).orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gold_daily_revenue = df_silver.groupBy(F.to_date("tpep_pickup_datetime").alias("date")) \
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"), 
        F.round(F.sum("tip_amount"), 2).alias("total_tips"),     
        F.count("*").alias("trip_count")
    ) \
    .withColumn("rolling_total_revenue", 
                F.sum("total_revenue").over(window_spec).cast("decimal(18,2)"))

spark.sql("DROP TABLE IF EXISTS workspace.default.gold_daily_revenue")
gold_daily_revenue.write.format("delta").mode("overwrite").saveAsTable("workspace.default.gold_daily_revenue")

# --- GOLD TOP ROUTES ---
gold_top_routes = df_silver.groupBy("PULocationID", "DOLocationID") \
    .agg(F.count("*").alias("trip_count")) \
    .orderBy(F.desc("trip_count")) \
    .limit(10)

spark.sql("DROP TABLE IF EXISTS workspace.default.gold_top_routes")
gold_top_routes.write.format("delta").mode("overwrite").saveAsTable("workspace.default.gold_top_routes")

# --- GOLD HOURLY ACTIVITY  ---
gold_hourly_activity = df_silver.withColumn("hour", F.hour("tpep_pickup_datetime")) \
    .groupBy("hour") \
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare") # Додали середній чек по годинах
    ) \
    .orderBy("hour")

spark.sql("DROP TABLE IF EXISTS workspace.default.gold_hourly_activity")
gold_hourly_activity.write.format("delta").mode("overwrite").saveAsTable("workspace.default.gold_hourly_activity")

# --- OPTIMIZE  ---
spark.sql("OPTIMIZE workspace.default.gold_daily_revenue")
spark.sql("OPTIMIZE workspace.default.gold_hourly_activity")

# Results
gold_hourly_activity.show(24)
gold_daily_revenue.orderBy("date").show(50)
gold_top_routes.show(10)