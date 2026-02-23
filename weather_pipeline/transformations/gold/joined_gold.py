from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name= "weather.gold.current_environmen_metrics",
    comment= "Final joined Materialized View of current weather and air quality for dashboards."
)
def current_environment_metrics():
    #Reading the silver tables 
    weather_df = spark.read.table("weather.silver.weather_silver")
    air_quality_df = spark.read.table("weather.silver.air_quality_silver")

    #Joining the silver tables
    gold_df = weather_df.join(
        air_quality_df,
        on = "city",
        how = "inner"
    ).select(
        weather_df["city"],
        weather_df["event_time"].alias("weather_updated_at"),
        weather_df["temperature"],
        weather_df["wind_speed"],
        weather_df["weather_code"],
        air_quality_df["pm10"],
        air_quality_df["pm2_5"],
        F.when(air_quality_df["pm10"] > 50, "High Pollution").otherwise("Good").alias("air_quality_status")
    )

    return gold_df
