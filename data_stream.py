import json
import os
import time

from pyspark.sql import SparkSession
import pyspark.sql.types as pst
import pyspark.sql.functions as psf


RADIO_CODE_JSON_FILEPATH = os.environ.get("RADIO_CODE_JSON_FILEPATH", "./radio_code.json")
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC = "udacity.project.spark-streaming.police"


schema = pst.StructType([
    pst.StructField("crime_id", pst.StringType()),  # : "183653763",
    pst.StructField("original_crime_type_name", pst.StringType()),  # : "Traffic Stop",
    pst.StructField("report_date", pst.DateType()),  # : "2018-12-31T00:00:00.000",
    pst.StructField("call_date", pst.DateType()),  # : "2018-12-31T00:00:00.000",
    pst.StructField("offense_date", pst.DateType()),  # : "2018-12-31T00:00:00.000",
    pst.StructField("call_time", pst.StringType()),  # : "23:57",
    pst.StructField("call_date_time", pst.TimestampType()),  # : "2018-12-31T23:57:00.000",
    pst.StructField("disposition", pst.StringType()),  # : "ADM",
    pst.StructField("address", pst.StringType()),  # : "Geary Bl/divisadero St",
    pst.StructField("city", pst.StringType()),  # : "San Francisco",
    pst.StructField("state", pst.StringType()),  # : "CA",
    pst.StructField("agency_id", pst.StringType()),  # : "1",
    pst.StructField("address_type", pst.StringType()),  # : "Intersection",
    pst.StructField("common_location", pst.StringType()),  # : ""
])


def run_spark_job(spark):

    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "udacity.project.spark-streaming.police")
        .option("startingOffsets", "earliest")
        # .option("kafkaConsumer.pollTimeoutMs", 3_000)
        .option("maxOffsetsPerTrigger", 6_000)
        .option("stopGracefullyOnShutdown", "true")
        .load()
    )

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df.select(
        psf.from_json(psf.col("value"), schema).alias("parsed")
    ).select("parsed.*")
    service_table.createOrReplaceTempView("service")

    window_agg_df = (
        service_table
        .withWatermark("call_date_time", "1 hour")
        .groupBy(
            psf.window(service_table.call_date_time, "30 minutes"),
            service_table.original_crime_type_name,
            service_table.disposition,
        ).count()
    )
    window_agg_df.createOrReplaceTempView("windowed_crime_types")

    query_q1 = (
        window_agg_df
        .writeStream
        .outputMode("update")
        .format("console")
        .start()
    )

    radio_code_df = spark.read.option("multiLine", True).json(RADIO_CODE_JSON_FILEPATH)
    radio_code_df.createOrReplaceTempView("radio_code")

    join_query = spark.sql("""
        SELECT ct.*, r.description
        FROM windowed_crime_types as ct
        LEFT JOIN radio_code as r on r.disposition_code = ct.disposition
    """)

    query_q2 = (
        join_query
        .writeStream
        .outputMode("update")
        .format("console")
        .start()
    )

    is_finished = False
    while not is_finished:
        time.sleep(10)
        if not query_q1.lastProgress:
            continue

        # we have 200_000 items in the source
        partitions = query_q1.lastProgress["sources"][0]["endOffset"][KAFKA_TOPIC]
        processed_offset = sum(partitions.values())
        is_finished = processed_offset == 200_000

        print(f"Last progress: {json.dumps(query_q1.lastProgress, indent=2, sort_keys=True)}")

    query_q1.stop()
    print("""

    #############
    inital kafka aggregation query reached offset 200_000 and was stopped
    #############

    """)
    query_q1.awaitTermination()
    query_q2.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    print("""

    #############
    spark session started
    #############

    """)

    run_spark_job(spark)

    spark.stop()
