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
# S3 PATHS
# ---------------------------
BUCKET        = "ttn-de-bootcamp-bronze-us-east-1"
SILVER_BUCKET = "ttn-de-bootcamp-silver-us-east-1"

RAW_PREFIX       = "deepak-bronze-07-04/vehicle_assignment/raw_files/"
PROCESSED_PREFIX = "deepak-bronze-07-04/vehicle_assignment/processed/"
CORRUPT_PREFIX   = "deepak-bronze-07-04/vehicle_assignment/corrupted_files/"

SILVER_PATH           = f"s3://{SILVER_BUCKET}/deepak-silver-07-04/vehicle_assignment/"
VEHICLE_REGISTRY_PATH = f"s3://{SILVER_BUCKET}/deepak-silver-07-04/vehicle_registry/"

EPOCH_MIN          = 946684800        # 2000-01-01
RATE_MIN, RATE_MAX = 300.0, 100000.0

s3 = boto3.client("s3")

# ---------------------------
# HELPER
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
# LOAD VEHICLE REGISTRY ONCE
# ---------------------------
try:
    registry_df = spark.read.parquet(VEHICLE_REGISTRY_PATH)
    valid_vins  = registry_df.select(upper(col("vin")).alias("vin")).distinct()
    print(f"✅ Registry loaded: {valid_vins.count()} valid VINs")
except Exception as e:
    raise RuntimeError(f"Failed to load Vehicle Registry: {str(e)}")

# ---------------------------
# LIST UNPROCESSED FILES
# ---------------------------
files_to_process = []
paginator = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=BUCKET, Prefix=RAW_PREFIX):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".csv"):
            filename = obj["Key"][len(RAW_PREFIX):]
            if not is_already_processed(filename, obj["ETag"]):
                files_to_process.append({
                    "key":      obj["Key"],
                    "filename": filename,
                    "etag":     obj["ETag"].replace('"', '')
                })
            else:
                print(f"Skipping (Already Processed): {filename}")

print(f"Files to process: {len(files_to_process)}")

# ---------------------------
# MAIN LOOP
# ---------------------------
for file_info in files_to_process:
    file_key  = file_info["key"]
    filename  = file_info["filename"]
    file_etag = file_info["etag"]
    file_path = f"s3://{BUCKET}/{file_key}"
    print(f"\nProcessing: {file_path}")

    try:
        # ── READ ─────────────────────────────────────────────────────────────
        raw_df = spark.read.option("header", True).csv(file_path)

        # ── STEP 1: NORMALIZE ────────────────────────────────────────────────
        NULL_SENTINELS = ["", "null", "n/a", "none", "nan"]
        for c in raw_df.columns:
            raw_df = raw_df.withColumn(c, trim(col(c)))
            raw_df = raw_df.withColumn(
                c,
                when(lower(col(c)).isin(NULL_SENTINELS) | col(c).isNull(), None)
                .otherwise(col(c))
            )

        df = (
            raw_df
            .withColumn("vin",        upper(col("vin")))
            .withColumn("daily_rate",
                round(regexp_replace(col("daily_rate"), ",", ".").cast("double"), 2))
            .withColumn("start_ts",
                from_unixtime(
                    regexp_replace(col("start_timestamp"), r"\..*$", "").cast("long")
                ).cast("timestamp"))
            .withColumn("end_ts",
                from_unixtime(
                    regexp_replace(col("end_timestamp"), r"\..*$", "").cast("long")
                ).cast("timestamp"))
        )

        # ── STEP 2: BASIC VALIDATION → CORRUPT ──────────────────────────────
        def flag(cond, msg):
            return when(cond, msg).otherwise("")

        reason = concat(
            flag(col("vin").isNull() | (length(col("vin")) != 8),
                 "Invalid VIN | "),
            flag(col("driver_id").isNull() |
                 ~col("driver_id").rlike("^DRV_[0-9]{4}$"),
                 "Invalid Driver ID | "),
            flag(col("start_ts").isNull() |
                 (unix_timestamp(col("start_ts")) < EPOCH_MIN),
                 "Invalid Start Time | "),
            flag(col("daily_rate").isNull() |
                 (col("daily_rate") < RATE_MIN) |
                 (col("daily_rate") > RATE_MAX),
                 "Rate Out of Range | "),
        )
        df = df.withColumn("corruption_reason",
                           regexp_replace(reason, r"\s*\|\s*$", ""))

        clean_df = df.filter(col("corruption_reason") == "").select(
            "vin", "driver_id",
            col("start_ts").alias("start_date"),
            col("end_ts").alias("end_date"),
            "daily_rate", "region"
        )
        corrupt_df = df.filter(col("corruption_reason") != "").select(
            "vin", "driver_id", "start_timestamp", "end_timestamp",
            "daily_rate", "corruption_reason"
        )

        # ── STEP 3: REGISTRY FILTER ──────────────────────────────────────────
        # Reject VINs not present in vehicle_registry silver
        unregistered_df = (
            clean_df
            .join(valid_vins, on="vin", how="left_anti")
            .withColumn("corruption_reason", lit("VIN Not Found in Vehicle Registry"))
            .withColumn("start_timestamp",   col("start_date").cast("string"))
            .withColumn("end_timestamp",     col("end_date").cast("string"))
            .select("vin", "driver_id", "start_timestamp", "end_timestamp",
                    "daily_rate", "corruption_reason")
        )
        if not unregistered_df.rdd.isEmpty():
            print(f"  ⚠️  {unregistered_df.count()} records rejected: VIN not in registry")
            corrupt_df = corrupt_df.unionByName(unregistered_df, allowMissingColumns=True)

        clean_df = clean_df.join(valid_vins, on="vin", how="inner")
        print(f"  ✅ After registry filter: {clean_df.count()} records")

        # ── STEP 4: RULE 2 — DUPLICATE VIN+TIME → KEEP HIGHER RATE ──────────
        # Only records whose time intervals genuinely overlap compete on rate.
        # Sequential non-overlapping assignments for the same VIN are untouched.
        a = clean_df.alias("a")
        b = clean_df.alias("b")

        losers = a.join(
            b,
            (col("a.vin")       == col("b.vin")) &
            (col("a.driver_id") != col("b.driver_id")) &
            (col("a.start_date") < coalesce(col("b.end_date"),
                                            lit("9999-12-31").cast("timestamp"))) &
            (coalesce(col("a.end_date"), lit("9999-12-31").cast("timestamp"))
                                        > col("b.start_date")) &
            (col("a.daily_rate") < col("b.daily_rate")),
            "inner"
        ).select(
            col("a.vin").alias("vin"),
            col("a.driver_id").alias("driver_id"),
            col("a.start_date").alias("start_date")
        ).distinct()

        if not losers.rdd.isEmpty():
            loser_count = losers.count()
            print(f"  ⚠️  {loser_count} records rejected: overlapping interval, lower rate")
            corrupt_df = corrupt_df.unionByName(
                clean_df
                .join(losers, on=["vin", "driver_id", "start_date"], how="inner")
                .withColumn("corruption_reason",
                            lit("Duplicate VIN Interval – Lower Rate Driver Rejected"))
                .withColumn("start_timestamp", col("start_date").cast("string"))
                .withColumn("end_timestamp",   col("end_date").cast("string"))
                .select("vin", "driver_id", "start_timestamp", "end_timestamp",
                        "daily_rate", "corruption_reason"),
                allowMissingColumns=True
            )
            clean_df = clean_df.join(
                losers, on=["vin", "driver_id", "start_date"], how="left_anti"
            )

        # ── STEP 5: MERGE INCOMING WITH EXISTING SILVER ──────────────────────
        # Load existing silver if it exists, otherwise start fresh.
        # We must combine incoming + existing BEFORE running SCD2 so that the
        # LEAD window sees the full history per VIN across both batches —
        # this fixes the cross-batch overlap problem from the previous version.
        try:
            existing_silver = spark.read.parquet(SILVER_PATH)

            # Drop ingestion_time from existing so it doesn't conflict
            # after we re-stamp it below on the full combined set.
            existing_silver = existing_silver.drop("ingestion_time", "status")

            # FIX: left_anti removes existing records that are being superseded
            # by an incoming record with the same (vin, driver_id, start_date).
            # Without this, unionByName would create duplicates for re-processed rows.
            existing_silver = existing_silver.join(
                clean_df.select("vin", "driver_id", "start_date"),
                on=["vin", "driver_id", "start_date"],
                how="left_anti"
            )

            combined_df = existing_silver.unionByName(clean_df, allowMissingColumns=True)
            print(f"  ✅ Combined existing + incoming: {combined_df.count()} records")

        except Exception:
            print(f"  ℹ️  No existing silver table. Initializing.")
            combined_df = clean_df

        # ── STEP 6: SCD TYPE 2 — LEAD OVER FULL COMBINED HISTORY ────────────
        # Now that combined_df has ALL records (existing + incoming) for every VIN,
        # the LEAD window sees the complete timeline and correctly aligns end dates
        # across batch boundaries — not just within the incoming file.
        #
        # For VIN KK3TAM4B example:
        #   existing silver:  DRV_A start=2012  end=NULL  (IN-TRANSIT)
        #   incoming:         DRV_B start=2013  end=NULL
        #                     DRV_C start=2014  end=NULL
        #
        # LEAD over combined produces:
        #   DRV_A  end=2013  ARCHIVED   ← cross-batch closure, previously missed
        #   DRV_B  end=2014  ARCHIVED
        #   DRV_C  end=NULL  IN-TRANSIT ← only the last driver stays open

        window_scd = Window.partitionBy("vin").orderBy("start_date")

        scd_df = (
            combined_df
            .withColumn("next_start_date", lead("start_date").over(window_scd))
            # If a next driver exists → close current record at that start_date.
            # Handles: NULL end_dates, overlapping end_dates, and correct end_dates.
            .withColumn("end_date",
                when(col("next_start_date").isNotNull(), col("next_start_date"))
                .otherwise(col("end_date"))
            )
            .withColumn("status",
                when(col("end_date").isNull(), lit("IN-TRANSIT"))
                .otherwise(lit("ARCHIVED"))
            )
            .drop("next_start_date")
            .withColumn("ingestion_time", current_timestamp())
        )

        # ── STEP 7: CROSS-BATCH OVERLAP CHECK → CORRUPT ──────────────────────
        # After SCD2 alignment, check if any record still has end_date <= start_date.
        # This catches edge cases like two records with identical start_dates
        # that survived Step 4 (equal rates, different drivers — both kept).
        bad_dates = scd_df.filter(
            col("end_date").isNotNull() & (col("end_date") <= col("start_date"))
        )
        if not bad_dates.rdd.isEmpty():
            print(f"  ⚠️  {bad_dates.count()} records with end <= start after SCD2 alignment")
            corrupt_df = corrupt_df.unionByName(
                bad_dates
                .withColumn("corruption_reason", lit("End Date before Start after SCD2 alignment"))
                .withColumn("start_timestamp", col("start_date").cast("string"))
                .withColumn("end_timestamp",   col("end_date").cast("string"))
                .select("vin", "driver_id", "start_timestamp", "end_timestamp",
                        "daily_rate", "corruption_reason"),
                allowMissingColumns=True
            )
            scd_df = scd_df.join(
                bad_dates.select("vin", "driver_id", "start_date"),
                on=["vin", "driver_id", "start_date"],
                how="left_anti"
            )

        # ── STEP 8: FINAL DEDUP ───────────────────────────────────────────────
        # FIX: safety net dedup on (vin, driver_id, start_date).
        # Keeps the most recently ingested version if any duplicates slipped through.
        window_dedup = Window.partitionBy("vin", "driver_id", "start_date") \
            .orderBy(col("ingestion_time").desc())

        final_silver = (
            scd_df
            .withColumn("rn", row_number().over(window_dedup))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        print(f"  ✅ Final silver records: {final_silver.count()}")

        # ── STEP 9: WRITE DIRECTLY TO SILVER ─────────────────────────────────
        final_silver.write.mode("overwrite").parquet(SILVER_PATH)

        # ── STEP 10: WRITE CORRUPT ────────────────────────────────────────────
        if not corrupt_df.rdd.isEmpty():
            corrupt_df.write.mode("append").option("header", True) \
                .csv(f"s3://{BUCKET}/{CORRUPT_PREFIX}")
            print(f"  ⚠️  Corrupt records written.")

        # ── STEP 11: MARKER FILE ──────────────────────────────────────────────
        s3.put_object(
            Bucket=BUCKET,
            Key=f"{PROCESSED_PREFIX}{filename}.{file_etag}.done",
            Body=f"Processed at {datetime.now().isoformat()}"
        )
        print(f"  ✅ Finished: {filename}")

    except Exception as e:
        import traceback
        print(f"  ❌ Error on {file_path}: {str(e)}")
        traceback.print_exc()

print("\nGlue Job Finished Successfully")