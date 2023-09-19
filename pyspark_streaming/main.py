from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, from_json, expr, window, count, lit, collect_list
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

jsonFormatSchema = (Path(__file__).parent / "schema-pageviews-value-v1.avsc").open("r").read()

if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-avro_2.12:3.4.1"
        )
        .master("local[4]")
        .appName("sparkApp")
        .getOrCreate()
    )

    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "pageviews")
        .option("startingOffsets", "latest")
        .load()
    )
    df.printSchema()

    parsed_df = (
        df
        .select(
            col("partition").cast(StringType()).alias("partition"),
            col("timestamp").cast(TimestampType()).alias("timestamp"),
            col("key").cast(StringType()).alias("key"),
            from_avro(expr("substring(value, 6)"), jsonFormatSchema).alias("payload")
        )
        .select(
            col("partition"),
            col("timestamp"),
            col("key"),
            col("payload.*"),
        )
    )

    parsed_df.printSchema()

    # (
    #     parsed_df
    #     .writeStream
    #     .format("console")
    #     .start()
    #     .awaitTermination()
    # )

    agg_df = (
        parsed_df
        .withWatermark("timestamp", "3 seconds")
        .groupBy(window("timestamp", windowDuration="5 seconds"), "partition", "userid")
        .agg(
            count(lit(1)).alias("count"),
            collect_list("pageid").alias("pageids")
        )
        .select(
            "partition",
            "userid",
            "count",
            "pageids",
        )
    )

    (
        agg_df
        .writeStream
        .format("console")
        .start()
        .awaitTermination()
    )
