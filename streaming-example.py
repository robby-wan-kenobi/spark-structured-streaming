# import findspark
# findspark.init()

import sys
import pyspark
import pyspark.sql.streaming

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, current_timestamp, col
from pyspark.sql.types import *

def get_streaming_input_df(spark, input_directory):
    data_input_directory = "data/input/" + input_directory + "/"

    squirrel_schema = (StructType()
        .add(StructField("AreaName", StringType()))
        .add(StructField("AreaID", StringType()))
        .add(StructField("ParkName", StringType()))
        .add(StructField("ParkID", IntegerType()))
        .add(StructField("SquirrelID", StringType()))
        .add(StructField("PrimaryFurColor", StringType()))
        .add(StructField("HighlightsInFurColor", StringType()))
        .add(StructField("ColorNotes", StringType()))
        .add(StructField("Location", StringType()))
        .add(StructField("AboveGroundInFeet", IntegerType()))
        .add(StructField("SpecificLocation", StringType()))
        .add(StructField("Activities", StringType()))
        .add(StructField("InteractionsWithHumans", StringType()))
        .add(StructField("OtherNotesOrObservations", StringType()))
        .add(StructField("SquirrelLatitude", FloatType()))
        .add(StructField("SquirrelLongitude", FloatType()))
    )
    park_schema = (StructType()
        .add(StructField("AreaName", StringType()))
        .add(StructField("AreaID", StringType()))
        .add(StructField("ParkName", StringType()))
        .add(StructField("ParkID", IntegerType()))
        .add(StructField("Date", TimestampType()))
        .add(StructField("StartTime", TimestampType()))
        .add(StructField("EndTime", TimestampType()))
        .add(StructField("TotalTimeInMinutes", IntegerType()))
        .add(StructField("ParkConditions", StringType()))
        .add(StructField("OtherAnimalSightings", StringType()))
        .add(StructField("Litter", StringType()))
        .add(StructField("TemperatureAndWeather", StringType()))
        .add(StructField("NumberOfSquirrels", IntegerType()))
        .add(StructField("SquirrelSighters", StringType()))
        .add(StructField("NumberOfSighters", IntegerType()))
    )

    if input_directory == "squirrel":
        schema = squirrel_schema
    elif input_directory == "park":
        schema = park_schema

    df = (
        spark
        .readStream
        .format("csv")
        .schema(schema)
        .option("header", "true")
        .load(data_input_directory)
    )

    return df

def get_once_input_df(spark, input_directory):
    data_input_directory = "data/input/" + input_directory + "/"
    
    squirrel_schema = (StructType()
        .add(StructField("AreaName", StringType()))
        .add(StructField("AreaID", StringType()))
        .add(StructField("ParkName", StringType()))
        .add(StructField("ParkID", IntegerType()))
        .add(StructField("SquirrelID", StringType()))
        .add(StructField("PrimaryFurColor", StringType()))
        .add(StructField("HighlightsInFurColor", StringType()))
        .add(StructField("ColorNotes", StringType()))
        .add(StructField("Location", StringType()))
        .add(StructField("AboveGroundInFeet", IntegerType()))
        .add(StructField("SpecificLocation", StringType()))
        .add(StructField("Activities", StringType()))
        .add(StructField("InteractionsWithHumans", StringType()))
        .add(StructField("OtherNotesOrObservations", StringType()))
        .add(StructField("SquirrelLatitude", FloatType()))
        .add(StructField("SquirrelLongitude", FloatType()))
    )
    park_schema = (StructType()
        .add(StructField("AreaName", StringType()))
        .add(StructField("AreaID", StringType()))
        .add(StructField("ParkName", StringType()))
        .add(StructField("ParkID", IntegerType()))
        .add(StructField("Date", TimestampType()))
        .add(StructField("StartTime", TimestampType()))
        .add(StructField("EndTime", TimestampType()))
        .add(StructField("TotalTimeInMinutes", IntegerType()))
        .add(StructField("ParkConditions", StringType()))
        .add(StructField("OtherAnimalSightings", StringType()))
        .add(StructField("Litter", StringType()))
        .add(StructField("TemperatureAndWeather", StringType()))
        .add(StructField("NumberOfSquirrels", IntegerType()))
        .add(StructField("SquirrelSighters", StringType()))
        .add(StructField("NumberOfSighters", IntegerType()))
    )

    if input_directory == "squirrel":
        schema = squirrel_schema
    elif input_directory == "park":
        schema = park_schema

    df = (
        spark
        .read
        .format("csv")
        .schema(schema)
        .option("header", "true")
        # .option("inferSchema", "true") # not possible with streaming
        .load(data_input_directory)
    )

    return df

def output_stream_df(spark, table, df):
    output_directory = "data/output/" + table + "/"
    checkpoint_directory = "data/output/" + table + "/checkpoints/"

    out_stream = (
        df
        .writeStream
        .format("console")
        # .format("csv")
        # .option("path", output_directory)
        .option("checkpointLocation", checkpoint_directory)
        .trigger(processingTime="5 seconds")
        .start()
    )
    out_stream.awaitTermination()

def output_once_df(spark, table, df):
    df.show(50, False)
    spark.stop()

if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: streaming-example <table>", file=sys.stderr)
    #     sys.exit(-1)

    spark = (
        SparkSession
        .builder
        .appName("StreamingExample")
        .getOrCreate()
    )

    stream = True
    # stream = False

    if stream:
        squirrel_df = get_streaming_input_df(spark, "squirrel")
        park_df = get_streaming_input_df(spark, "park")
        # output_stream_df(spark, "squirrel", squirrel_df)
    else:
        squirrel_df = get_once_input_df(spark, "squirrel")
        park_df = get_once_input_df(spark, "park")

    
    # squirrel_count_df = (
    #     squirrel_df
    #     .withColumn("timestamp", current_timestamp().cast(TimestampType()))
    #     .withWatermark("timestamp", "5 seconds")
    #     .groupBy("ParkName", "PrimaryFurColor", "timestamp")
    #     .agg(count("SquirrelID").alias("Total"))
    #     .select("ParkName", "PrimaryFurColor", "Total", "timestamp")
    # )
    dim_squirrel_df = (
        squirrel_df.alias("s")
        .join(park_df.alias("p"), "ParkID")
        .select(
            col("s.SquirrelID"), 
            col("p.ParkName"),
            col("p.AreaName"),
            col("s.PrimaryFurColor"),
            col("s.HighlightsInFurColor")
        )
    )

    if stream:
        output_stream_df(spark, "dim_squirrel", dim_squirrel_df)
    else:
        output_once_df(spark, "dim_squirrel", dim_squirrel_df)

