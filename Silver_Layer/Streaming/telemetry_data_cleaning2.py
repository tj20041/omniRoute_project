from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("Silver_Telemetry_Cleaning_With_Assignment_Validation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# SCHEMA
# ---------------------------
schema = StructType() \
    .add("vin", StringType()) \
    .add("driver_id", StringType()) \
    .add("speed", IntegerType()) \
    .add("lat", DoubleType()) \
    .add("long", DoubleType()) \
    .add("event_timestamp", StringType())

# ---------------------------
# READ RAW TELEMETRY (Bronze)
# ---------------------------
bronze_df = spark.read \
    .schema(schema) \
    .json(
        "s3://ttn-de-bootcamp-bronze-us-east-1/deepak-bronze-07-04/telemetry/raw_stream/"
    )

# ---------------------------
# VIN CLEANING & TS PARSING
# ---------------------------
bronze_df = bronze_df.withColumn(
    "vin",
    upper(trim(col("vin")))
)

bronze_df = bronze_df.withColumn(
    "parsed_ts",
    to_timestamp(col("event_timestamp"))
)

# ============================================================
# STEP 1: BASIC CORRUPTION LOGIC
# ============================================================
corrupt_df = bronze_df.withColumn(
    "corruption_reason",
    when(col("vin").isNull(), "VIN_NULL")
    .when(~col("vin").rlike("^[A-Z0-9]{8}$"), "VIN_INVALID")
    .when(col("driver_id").isNull(), "DRIVER_NULL")
    .when(~upper(col("driver_id")).rlike("^DRV_[0-9]+$"), "DRIVER_INVALID")
    .when(col("parsed_ts").isNull(), "TIMESTAMP_INVALID")
    .when(~((col("speed") >= 0) & (col("speed") <= 200)), "SPEED_INVALID")
    .when(~((col("lat") >= -90) & (col("lat") <= 90)), "LAT_INVALID")
    .when(~((col("long") >= -180) & (col("long") <= 180)), "LONG_INVALID")
    .otherwise(None)
)

initial_bad_df = corrupt_df.filter(
    col("corruption_reason").isNotNull()
)

potential_good_df = corrupt_df.filter(
    col("corruption_reason").isNull()
)

print(f"📊 Total records              : {bronze_df.count()}")
print(f"❌ Basic corrupt count        : {initial_bad_df.count()}")
print(f"🔍 Going to assignment check  : {potential_good_df.count()}")

# ============================================================
# STEP 2: ASSIGNMENT VALIDATION
# ============================================================
assign_df = spark.read.parquet(
    "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/vehicle_assignment/"
).select(
    upper(col("vin")).alias("assign_vin"),
    upper(col("driver_id")).alias("assign_driver_id"),
    to_timestamp(col("start_date")).alias("start_date"),
    to_timestamp(col("end_date")).alias("end_date")
)

potential_good_df = potential_good_df.withColumn(
    "driver_id_upper",
    upper(col("driver_id"))
)

join_condition = (
    (potential_good_df["vin"] == assign_df["assign_vin"]) &
    (potential_good_df["driver_id_upper"] == assign_df["assign_driver_id"]) &
    (potential_good_df["parsed_ts"] >= assign_df["start_date"]) &
    (
        assign_df["end_date"].isNull() |
        (potential_good_df["parsed_ts"] <= assign_df["end_date"])
    )
)

valid_telemetry_df = potential_good_df.join(
    assign_df,
    join_condition,
    "inner"
).select(
    potential_good_df["*"]
).drop(
    "driver_id_upper"
)

unassigned_bad_df = potential_good_df.join(
    assign_df,
    join_condition,
    "left_anti"
).drop(
    "driver_id_upper"
).withColumn(
    "corruption_reason",
    lit("ASSIGNMENT_NOT_FOUND")
)

print(f"✅ Valid after assignment check : {valid_telemetry_df.count()}")
print(f"❌ Rejected by assignment check : {unassigned_bad_df.count()}")

# ============================================================
# WRITE ALL CORRUPTED DATA
# ============================================================
final_bad_df = initial_bad_df.unionByName(
    unassigned_bad_df,
    allowMissingColumns=True
).withColumn(
    "corrupt_date",
    date_format(current_timestamp(), "yyyy-MM-dd")
)

final_bad_df.write \
    .mode("append") \
    .format("parquet") \
    .partitionBy("corrupt_date", "corruption_reason") \
    .save(
        "s3://ttn-de-bootcamp-bronze-us-east-1/deepak-bronze-07-04/telemetry/corrupted/"
    )

print("❌ Total Corrupted records written:", final_bad_df.count())

# ============================================================
# STEP 3: FINAL CLEAN DATA → SILVER
# ============================================================
silver_df = valid_telemetry_df \
    .withColumn("event_timestamp", col("parsed_ts")) \
    .drop("parsed_ts", "corruption_reason") \
    .dropDuplicates(["vin", "event_timestamp"]) \
    .withColumn(
        "speed_flag",
        when(col("speed") > 110, "HIGH").otherwise("NORMAL")
    ) \
    .withColumn(
        "event_date",
        date_format(col("event_timestamp"), "yyyy-MM-dd")
    )

silver_df.cache()

print("✅ Validated Clean records count:", silver_df.count())

(
    silver_df
    .withColumn(
        "month",
        substring(col("event_date"), 1, 7)
    )
    .write
    .mode("append")
    .format("parquet")
    .partitionBy("month", "speed_flag")
    .save(
        "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/telemetry/clean_data/"
    )
)

print("✅ Silver Telemetry write complete")

spark.stop()
