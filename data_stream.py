import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date


schema = StructType([
            StructField("crime_id", StringType(), True),
            StructField("original_crime_type_name", StringType(), True),
            StructField("report_date", StringType(), True),
            StructField("call_date", StringType(), True),
            StructField("offense_date", StringType(), True),
            StructField("call_time", StringType(), True),
            StructField("call_date_time", StringType(), True),
            StructField("disposition", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("agency_id", StringType(), True),
            StructField("address_type", StringType(), True),
            StructField("common_location", StringType(), True)])

@psf.udf(StringType())
def udf_convert_time(timestamp):
    d_str = str(timestamp)
    d = datetime.strptime(d_str, '%Y-%m-%dT%H:%M:%S.000')
    d = d.strftime('%Y%m%y%H')
    return d

def run_spark_job(spark):

    df = spark.readStream.format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "service-calls") \
                .option("maxOffsetPerTrigger", "200") \
                .option("startingOffsets", "earliest") \
                .load()

    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_CALLS"))\
        .select("SERVICE_CALLS.*")

    distinct_table = service_table\
        .select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_date_time'),
                psf.col('address'),
                psf.col('disposition'))

    counts_df = distinct_table \
        .withWatermark("call_date_time", "60 minutes") \
        .groupBy(
            psf.window(psf.col("call_date_time"), "60 minutes"),
            psf.col("original_crime_type_name")
        ).count()

    converted_df = distinct_table.withColumn("call_date_time", udf_convert_time(psf.col("call_date_time")))

    calls_per_2_days = distinct_table \
        .withWatermark("call_date_time", "2 days") \
        .groupBy(
            psf.window(psf.col("call_date_time"), "2 days")
        ).count()


    query_1 = counts_df \
            .writeStream \
            .outputMode('complete') \
            .format('console') \
            .option('truncate', 'false') \
            .start()


    query_2 = calls_per_2_days \
            .writeStream \
            .outputMode('complete') \
            .format('console') \
            .option('truncate', 'false') \
            .start()

    query_1.awaitTermination()
    query_2.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
                .builder \
                .appName('SF Crime Statistics with Spark Streaming') \
                .master("local[2]") \
                .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark started")
    logger.setLevel("WARN")

    run_spark_job(spark)

    spark.stop()



