from pyspark import pipelines as dp
from pyspark.sql import functions as F

# 1. Create a View that streams from your parsed Bronze table
@dp.view(
    name="air_quality_silver_source",
    comment="View reading from the parsed and validated bronze layer."
)
@dp.expect_or_drop("city_not_null", "city IS NOT NULL")
@dp.expect_or_drop("ingestion_ts_not_null", "ingestion_ts IS NOT NULL")
def air_quality_silver_source():
    # Reading from the table you defined in the previous step
    silver_prased_df = (
        spark.readStream.table("weather.bronze.air_quality_stream")
        .select(
            F.col("city"),
            F.col("ingestion_ts"),
            #Fetching the data from nested JSON structure
            F.col("raw_air_quality_data.current.time").alias("event_time"),
            F.col("raw_air_quality_data.current.pm10").alias("pm10"),
            F.col("raw_air_quality_data.current.pm2_5").alias("pm2_5")
        )
    )

    return silver_prased_df

# 2. Explicitly declare your target Silver streaming table
dp.create_streaming_table(
    name="weather.silver.air_quality_silver",
    comment="Deduplicated air quality data keeping only the latest state per city."
)

# 3. Create the CDC (Change Data Capture) Flow
dp.create_auto_cdc_flow(
    target = "weather.silver.air_quality_silver",
    source = "air_quality_silver_source",
    keys = ["city"], # We want exactly one row per city
    sequence_by = F.col("ingestion_ts"), # Resolve conflicts using the latest timestamp
    stored_as_scd_type = 1 # Overwrite old data with the newest data
)