from config import configuration
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, DataFrame, date_format, from_json, from_utc_timestamp
from pyspark.sql.types import FloatType, StructType, StructField, StringType, TimestampType, IntegerType


def main():
    spark = SparkSession.builder.appName("SeoulPopulation") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration["AWS_ACCESS_KEY"]) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration["AWS_SECRET_KEY"]) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    seoul_population_schema = StructType([
        StructField("current_time", TimestampType(), True),
        StructField("area_name", StringType(), False),
        StructField("area_code", StringType(), False),
        # StructField("live_population", IntegerType(), False),
        StructField("area_congest_level", StringType(), False),
        StructField("area_congest_message", StringType(), False),
        StructField("area_population_min", IntegerType(), False),
        StructField("area_population_max", IntegerType(), False),
        StructField("male_population_rate", FloatType(), False),
        StructField("female_population_rate", FloatType(), False),
        StructField("population_rate_0", FloatType(), False),
        StructField("population_rate_10", FloatType(), False),
        StructField("population_rate_20", FloatType(), False),
        StructField("population_rate_30", FloatType(), False),
        StructField("population_rate_40", FloatType(), False),
        StructField("population_rate_50", FloatType(), False),
        StructField("population_rate_60", FloatType(), False),
        StructField("population_rate_70", FloatType(), False),
        StructField("resident_population_rate", FloatType(), False),
        StructField("non_resident_population_rate", FloatType(), False)
    ])

    def read_kafka(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "broker:29092")
                .option('subscribe', topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withWatermark("current_time", '2 minutes')
                )

    def stream_writer(input: DataFrame, checkpoint_folder, output):
        return (input
                .withColumn("current_time", from_utc_timestamp(col("current_time"), "Asia/Seoul"))
                .withColumn("year", date_format(col("current_time"), "yyyy"))
                .withColumn("month", date_format(col("current_time"), "MM"))
                .withColumn("day", date_format(col("current_time"), "dd"))
                .withColumn("hour", date_format(col("current_time"), "HH"))
                .withColumn("minute", date_format(col("current_time"), "mm"))
                .writeStream
                .format("parquet")
                .option("checkpointLocation", checkpoint_folder)
                .option("path", output)
                .partitionBy("year", "month", "day", "hour", "minute")
                .outputMode("append")
                .start()
                )

    seoul_population_dataframe = read_kafka("seoul_population", seoul_population_schema).alias("seoul_population")
    query = stream_writer(seoul_population_dataframe, configuration["AWS_CHECKPOINT_FOLDER"],
                          configuration["AWS_OUTPUT"])
    query.awaitTermination()


if __name__ == "__main__":
    main()
