

import pytest
from src.transformations import clean_taxi_data

def test_clean_taxi_data_filters_negative_distance(spark):
    # Створюємо тестові дані
    data = [("2025-01-01 10:00:00", "2025-01-01 10:10:00", -5.0, 10.0), # Має відфільтруватись
            ("2025-01-01 11:00:00", "2025-01-01 11:10:00", 2.0, 15.0)]   # Має залишитись
    columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "total_amount"]
    df = spark.createDataFrame(data, columns)
    
    result_df = clean_taxi_data(df)
    
    assert result_df.count() == 1
    assert result_df.collect()[0]["trip_distance"] == 2.0