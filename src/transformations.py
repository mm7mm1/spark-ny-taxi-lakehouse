
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def clean_taxi_data(df):
    """Трансформація для Silver шару"""
    return df \
        .filter(
            (F.year(F.col("tpep_pickup_datetime")) == 2025) & 
            (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) &
            (F.col("trip_distance") > 0) & (F.col("trip_distance") < 500) &
            (F.abs(F.col("total_amount")) < 5000)
        ) \
        .withColumn("fare_amount", F.round(F.when(F.abs(F.col("fare_amount")) == 0, F.lit(None)).otherwise(F.abs(F.col("fare_amount"))), 3)) \
        .withColumn("passenger_count", F.when(F.col("passenger_count") == 0, F.lit(None)).otherwise(F.col("passenger_count"))) \
        .withColumn("trip_duration_minutes", F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60, 3)) \
        .filter("trip_duration_minutes < 1440") \
        .withColumn("pickup_month", F.month("tpep_pickup_datetime")) \
        .dropDuplicates()

def get_daily_revenue(df):
    """Трансформація для Gold: щоденна виручка з накопиченням"""
    window_spec = Window.partitionBy(F.lit(1)).orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    return df.groupBy(F.to_date("tpep_pickup_datetime").alias("date")) \
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue"), 
            F.count("*").alias("trip_count")
        ) \
        .withColumn("rolling_total_revenue", F.sum("total_revenue").over(window_spec).cast("decimal(18,2)"))