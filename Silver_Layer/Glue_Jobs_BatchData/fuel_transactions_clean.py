# ---------------------------
# AWS GLUE SETUP
# ---------------------------
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import boto3
from datetime import datetime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ---------------------------
# S3 CONFIGURATION
# ---------------------------
BUCKET        = "ttn-de-bootcamp-bronze-us-east-1"
SILVER_BUCKET = "ttn-de-bootcamp-silver-us-east-1"

RAW_PREFIX       = "deepak-bronze-07-04/fuel_transactions/raw_files/"
PROCESSED_PREFIX = "deepak-bronze-07-04/fuel_transactions/processed/"
CORRUPT_PREFIX   = "deepak-bronze-07-04/fuel_transactions/corrupted_files/"

SILVER_PATH           = f"s3://{SILVER_BUCKET}/deepak-silver-07-04/fuel_transactions/"
VEHICLE_REGISTRY_PATH = f"s3://{SILVER_BUCKET}/deepak-silver-07-04/vehicle_registry/"

s3 = boto3.client("s3")

# ---------------------------
# THRESHOLDS
# ---------------------------
FUEL_MIN_LITERS    = 1.0
FUEL_MAX_LITERS    = 1000.0
ODOMETER_MAX_KM    = 2000000
TIMESTAMP_MIN_YEAR = 2000
TIMESTAMP_MAX_YEAR = 2029
SAME_VIN_GAP_MINS  = 60

# ---------------------------
# HELPER: CHECK IF PROCESSED
# ---------------------------
def is_already_processed(filename, etag):
    clean_etag = etag.replace('"', '')
    marker_key = f"{PROCESSED_PREFIX}{filename}.{clean_etag}.done"
    try:
        s3.head_object(Bucket=BUCKET, Key=marker_key)
        return True
    except:
        return False

# ---------------------------
# ✅ LOAD VEHICLE REGISTRY ONCE (outside loop for efficiency)
# ---------------------------
try:
    registry_df = spark.read.parquet(VEHICLE_REGISTRY_PATH)
    valid_vins  = registry_df.select(upper(col("vin")).alias("vin")).distinct()
    print(f"Vehicle Registry loaded. Total valid VINs: {valid_vins.count()}")
except Exception as e:
    raise RuntimeError(f"Failed to load Vehicle Registry from {VEHICLE_REGISTRY_PATH}: {str(e)}")

# ---------------------------
# LIST FILES
# ---------------------------
files_to_process = []
paginator = s3.get_paginator("list_objects_v2")

print("Checking for new fuel transaction files...")
for page in paginator.paginate(Bucket=BUCKET, Prefix=RAW_PREFIX):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            filename = key[len(RAW_PREFIX):]
            if not is_already_processed(filename, obj["ETag"]):
                files_to_process.append({
                    "key":      key,
                    "filename": filename,
                    "etag":     obj["ETag"].replace('"', '')
                })
            else:
                print(f"Skipping (Already Processed): {filename}")

print(f"Total files to process: {len(files_to_process)}")

# ---------------------------
# MAIN PROCESSING LOOP
# ---------------------------
for file_info in files_to_process:
    file_key  = file_info["key"]
    filename  = file_info["filename"]
    file_etag = file_info["etag"]

    file_path = f"s3://{BUCKET}/{file_key}"
    print(f"\nProcessing file: {file_path}")

    try:
        # READ FILE
        df = spark.read.option("header", True).csv(file_path)

        # 1. NORMALIZATION & NULL HANDLING
        NULL_SENTINELS = ["", "null", "n/a", "nan", "none"]
        for c in df.columns:
            df = df.withColumn(c, trim(col(c)))
            df = df.withColumn(
                c,
                when(lower(col(c)).isin(NULL_SENTINELS) | col(c).isNull(), None).otherwise(col(c))
            )

        # 2. TIMESTAMP PARSING LOGIC
        df = df.withColumn("timestamp_clean", regexp_replace(col("timestamp"), r"([+-]\d{2}:\d{2}|Z)$", ""))
        df = df.withColumn("timestamp_clean", regexp_replace(col("timestamp_clean"), "T", " "))
        df = df.withColumn("timestamp_clean",
            when(col("timestamp_clean").rlike(r"^\d{4}-\d{2}-\d{2}$"),
                 concat(col("timestamp_clean"), lit(" 07:00:00")))
            .otherwise(col("timestamp_clean")))
        df = df.withColumn("timestamp_parsed",
            when(col("timestamp_clean").rlike(r"^\d{13}$"),
                 to_timestamp((col("timestamp_clean").cast("long") / 1000)))
            .when(col("timestamp_clean").rlike(r"^\d{10}$"),
                 to_timestamp(col("timestamp_clean").cast("long")))
            .otherwise(to_timestamp(col("timestamp_clean"), "yyyy-MM-dd HH:mm:ss"))
        )

        # 3. FIELD CONVERSIONS
        df = df.withColumn("fuel_liters_clean", round(regexp_replace(col("fuel_liters"), ",", ".").cast("double"), 2))
        df = df.withColumn("odo_clean",         col("odometer_reading").cast("double"))
        df = df.withColumn("vin_upper",          upper(col("vin")))

        # 4. CORRUPTION FLAGS
        def flag(cond, msg):
            return when(cond, msg).otherwise("")

        reason = concat(
            flag(col("transaction_id").isNull() | ~col("transaction_id").rlike("^TXN_[0-9]{7}$"),          "Invalid TXN_ID | "),
            flag(col("vin_upper").isNull() | (length(col("vin_upper")) != 8),                               "Invalid VIN | "),
            flag(col("timestamp_parsed").isNull() |
                 (year(col("timestamp_parsed")) < TIMESTAMP_MIN_YEAR) |
                 (year(col("timestamp_parsed")) > TIMESTAMP_MAX_YEAR),                                      "Invalid Timestamp | "),
            flag(col("fuel_liters_clean").isNull() |
                 (col("fuel_liters_clean") < FUEL_MIN_LITERS) |
                 (col("fuel_liters_clean") > FUEL_MAX_LITERS),                                              "Fuel out of range | "),
            flag(col("odo_clean").isNull() | (col("odo_clean") <= 0) | (col("odo_clean") > ODOMETER_MAX_KM), "Odometer out of range | "),
            flag((col("fuel_liters_clean") > 1000) & (col("odo_clean") < 1000),                             "Potential Fuel/Odo Swap | ")
        )
        df = df.withColumn("corruption_reason", regexp_replace(reason, r"\s*\|\s*$", ""))

        # 5. INTRA-FILE DEDUPLICATION (Transaction ID)
        txn_window = Window.partitionBy("transaction_id").orderBy(lit(1))
        df = df.withColumn("txn_rn", row_number().over(txn_window))

        duplicates_txn = df.filter(col("txn_rn") > 1).withColumn("corruption_reason", lit("Duplicate Transaction ID"))
        df = df.filter(col("txn_rn") == 1).drop("txn_rn")

        # 6. TIME-GAP VALIDATION (Same VIN gap < 60 mins)
        vin_time_window = Window.partitionBy("vin_upper").orderBy("timestamp_parsed")
        df = df.withColumn("prev_ts",  lag("timestamp_parsed").over(vin_time_window))
        df = df.withColumn("gap_mins", (unix_timestamp("timestamp_parsed") - unix_timestamp("prev_ts")) / 60)
        df = df.withColumn("corruption_reason",
            when((col("gap_mins").isNotNull()) & (col("gap_mins") < SAME_VIN_GAP_MINS),
                 concat(col("corruption_reason"), lit(" | Refuel gap < 60min")))
            .otherwise(col("corruption_reason")))

        # 7. SPLIT: clean vs corrupt (based on all checks so far)
        clean_df   = df.filter(col("corruption_reason") == "")
        corrupt_df = df.filter(col("corruption_reason") != "").unionByName(duplicates_txn, allowMissingColumns=True)

        # ── ✅ 7b. VEHICLE REGISTRY FILTER ────────────────────────────────────
        # Reject any clean record whose VIN is not present in the vehicle registry.
        # These are routed to corrupt_df; only registry-matched VINs proceed to silver.
        unregistered_df = (
            clean_df
            .join(valid_vins, on=clean_df["vin_upper"] == valid_vins["vin"], how="left_anti")
            .withColumn("corruption_reason", lit("VIN Not Found in Vehicle Registry"))
            .select("transaction_id", "vin", "fuel_liters", "odometer_reading", "timestamp", "corruption_reason")
        )
        if not unregistered_df.rdd.isEmpty():
            print(f"  ⚠️  {unregistered_df.count()} record(s) rejected: VIN not in registry")
            corrupt_df = corrupt_df.unionByName(unregistered_df, allowMissingColumns=True)

        # Keep only registry-matched VINs for silver
        clean_df = clean_df.join(valid_vins, on=clean_df["vin_upper"] == valid_vins["vin"], how="inner").drop(valid_vins["vin"])
        print(f"  ✅ Records passing registry check: {clean_df.count()}")
        # ── end registry filter ───────────────────────────────────────────────

        # 8. WRITE CLEAN DATA TO SILVER
        if not clean_df.rdd.isEmpty():
            final_clean = clean_df.select(
                "transaction_id",
                col("vin_upper").alias("vin"),
                col("fuel_liters_clean").alias("fuel_liters"),
                col("odo_clean").alias("odometer_reading"),
                col("timestamp_parsed").alias("timestamp"),
                current_timestamp().alias("ingestion_time")
            )
            final_clean.write.mode("append").parquet(SILVER_PATH)

        # 9. WRITE CORRUPT DATA TO BRONZE
        if not corrupt_df.rdd.isEmpty():
            final_corrupt = corrupt_df.select(
                "transaction_id", "vin", "fuel_liters", "odometer_reading", "timestamp", "corruption_reason"
            )
            final_corrupt.write.mode("append").option("header", True).csv(f"s3://{BUCKET}/{CORRUPT_PREFIX}")

        # 10. CREATE MARKER FILE
        marker_key = f"{PROCESSED_PREFIX}{filename}.{file_etag}.done"
        s3.put_object(
            Bucket=BUCKET,
            Key=marker_key,
            Body=f"Processed on {datetime.now().isoformat()}"
        )
        print(f"  ✅ Finished: {filename}")

    except Exception as e:
        print(f"  ❌ Error on {file_key}: {str(e)}")

print("\nGlue Job Finished")