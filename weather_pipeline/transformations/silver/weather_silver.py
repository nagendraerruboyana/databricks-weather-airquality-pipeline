from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(
    name= "weather_silver_source",
    comment= "View reading from the prased and validated bronze layer"
)
@dp.expect_or_drop("city_not_null", "city IS NOT NULL")
@dp.expect_or_drop("ingestion_ts_not_null", "ingestion_ts IS NOT NULL")
def weather_silver_source():
    silver_df = (
        spark.readStream.table("weather.bronze.weather_bronze_stream")
        .select(
            F.col("city"),
            F.col("ingestion_ts"),
            #Fetching the data from nested JSON structure
            F.col("raw_weather_data.current_weather.time").alias("event_time"),
            F.col("raw_weather_data.current_weather.temperature").alias("temperature"),
            F.col("raw_weather_data.current_weather.windspeed").alias("wind_speed"),
            F.col("raw_weather_data.current_weather.winddirection").alias("wind_direction"),
            F.col("raw_weather_data.current_weather.is_day").alias("is_day"),
            F.col("raw_weather_data.current_weather.weathercode").alias("weather_code")
        )
    )

    return silver_df

dp.create_streaming_table(
    name= "weather.silver.weather_silver",
    comment= "Deduplicated weather data keeping only the latest record per city"
)

dp.create_auto_cdc_flow(
    target= "weather.silver.weather_silver",
    source= "weather_silver_source",
    keys= ["city"],
    sequence_by= F.col("ingestion_ts"),
    stored_as_scd_type= 1
)
    