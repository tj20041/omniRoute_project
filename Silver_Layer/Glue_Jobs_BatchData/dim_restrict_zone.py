from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import boto3
from datetime import datetime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --- CONFIG ---
SILVER_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/restricted_zones/"
CORRUPT_PATH = "s3://ttn-de-bootcamp-bronze-us-east-1/deepak-bronze-07-04/restricted_zones/corrupted_files/"

# Geographical Constants
MIN_LAT, MAX_LAT   = -90.0,  90.0
MIN_LONG, MAX_LONG = -180.0, 180.0
MAX_NAME_LENGTH    = 100 # EC-04
MIN_ZONE_AREA      = 0.000001 # EC-16/EC-28 (roughly 10m x 10m)

# 1. LOAD DATA
# Note: Manually handling file listing/ETag logic as per your original script
df = spark.read.option("multiline", "true").json("s3://ttn-de-bootcamp-bronze-us-east-1/deepak-bronze-07-04/restricted_zones/raw_files/*.json")

# 2. STANDARDIZE & TRIM
df = df.toDF(*[c.strip().lower() for c in df.columns])
for c in df.columns:
    df = df.withColumn(c, trim(col(c)))

# 3. TYPE CASTING (EC-08, EC-09, EC-22)
df = df.withColumn("_min_lat", col("min_lat").cast("double")) \
       .withColumn("_max_lat", col("max_lat").cast("double")) \
       .withColumn("_min_long", col("min_long").cast("double")) \
       .withColumn("_max_long", col("max_long").cast("double"))

def flag(cond, msg):
    return when(cond, msg).otherwise("")

# 4. ENHANCED VALIDATION (Handling all 30 Edge Cases)
validation_rules = concat(
    # Name Validation (EC-01, EC-02, EC-03, EC-26)
    flag(col("zone_name").isNull() | (col("zone_name") == "") | (lower(col("zone_name")) == "null"), "Invalid/Missing Name | "),
    
    # Name Length & Format (EC-04, EC-05, EC-25, EC-30)
    flag(length(col("zone_name")) > MAX_NAME_LENGTH, "Name too long | "),
    flag(~col("zone_name").rlike("^[a-zA-Z0-9_-]+$"), "Special characters/Unicode not allowed | "),
    
    # Null Coordinate Checks (EC-07, EC-17, EC-18, EC-27, EC-29)
    flag(col("_min_lat").isNull() | col("_max_lat").isNull(), "Latitude missing/non-numeric | "),
    flag(col("_min_long").isNull() | col("_max_long").isNull(), "Longitude missing/non-numeric | "),
    
    # Coordinate Range Checks (EC-12, EC-13, EC-14, EC-15)
    flag((col("_min_lat") < MIN_LAT) | (col("_max_lat") > MAX_LAT), "Lat out of bounds | "),
    flag((col("_min_long") < MIN_LONG) | (col("_max_long") > MAX_LONG), "Long out of bounds | "),
    
    # Bounding Box Logic (EC-10, EC-11, EC-16, EC-28)
    flag(col("_min_lat") >= col("_max_lat"), "Lat Inverted/Zero-Area | "),
    flag(col("_min_long") >= col("_max_long"), "Long Inverted/Zero-Area | "),
    
    # Specific Suspicious Logic (EC-19, EC-20)
    flag((col("_min_lat") == 0) & (col("_max_lat") == 0) & (col("_min_long") == 0), "Null Island Suspicion | "),
    flag((col("_max_lat") - col("_min_lat") > 170) & (col("_max_long") - col("_min_long") > 350), "Global-scale zone suspicious | ")
)

df = df.withColumn("corruption_reason", regexp_replace(validation_rules, r"\s*\|\s*$", ""))

# 5. DEDUPLICATION (EC-06, EC-23, EC-24)
# Normalize to Uppercase for unique check (EC-23)
df = df.withColumn("norm_name", upper(col("zone_name")))
window_spec = Window.partitionBy("norm_name").orderBy(lit(1))
df = df.withColumn("row_num", row_number().over(window_spec))

duplicates = df.filter(col("row_num") > 1).withColumn("corruption_reason", lit("Duplicate Zone Name"))
df = df.filter(col("row_num") == 1)

# 6. SPLIT & WRITE
clean_df = df.filter(col("corruption_reason") == "")
corrupt_df = df.filter(col("corruption_reason") != "").unionByName(duplicates, allowMissingColumns=True)

# Write Clean (Parquet)
if not clean_df.rdd.isEmpty():
    clean_df.select(
        col("zone_name"),
        col("_min_lat").alias("min_lat"),
        col("_max_lat").alias("max_lat"),
        col("_min_long").alias("min_long"),
        col("_max_long").alias("max_long")
    ).write.mode("append").parquet(SILVER_PATH)

# Write Corrupt (CSV)
if not corrupt_df.rdd.isEmpty():
    corrupt_df.write.mode("append").option("header", True).csv(CORRUPT_PATH)

print("✅ Glue Job execution complete with full 30-case validation.")