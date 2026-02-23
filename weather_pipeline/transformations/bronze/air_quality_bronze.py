from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_PATH = "/Volumes/weather/raw_data/data_files/air_quality/"

@dp.table(
    name= "weather.bronze.air_quality_stream",
    comment= "Streaming ingestion of air quality data with Auto Loader"
)
def air_quality_data():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(SOURCE_PATH)
    )

    df = (
        df.withColumn("file_name", F.col("_metadata.file_name"))
        .withColumn("processing_ts", F.current_timestamp())
    )

    return df