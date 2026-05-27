from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Kafka_to_Bronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# schema of kafka message
schema = StructType([
    StructField("vin", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("event_timestamp", StringType(), True)
])

# Read Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "3.95.133.27:9092") \
    .option("subscribe", "telemetry-stream") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "1000") \
    .load()

# Parse actual JSON
bronze_df = kafka_df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# Debug
query_console = bronze_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Write to S3
bronze_query = bronze_df.writeStream \
    .format("json") \
    .outputMode("append") \
    .option(
        "path",
        "s3://ttn-de-bootcamp-bronze-us-east-1/deepak-bronze-07-04/telemetry/raw_stream/"
    ) \
    .option(
        "checkpointLocation",
        "s3://ttn-de-bootcamp-bronze-us-east-1/deepak-bronze-07-04/telemetry/checkpoint-bronze/"
    ) \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()