# ---------------------------
# AWS GLUE SETUP
# ---------------------------
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import boto3
from datetime import datetime

# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ---------------------------
# S3 CONFIGURATION
# ---------------------------
BUCKET = "ttn-de-bootcamp-bronze-us-east-1"

# Folder paths (Prefixes)
RAW_PREFIX       = "deepak-bronze-07-04/maintenance_schedules/raw_files/"
PROCESSED_PREFIX = "deepak-bronze-07-04/maintenance_schedules/processed/"
CORRUPT_PREFIX   = "deepak-bronze-07-04/maintenance_schedules/corrupted_files/"

# Destination path for Silver Layer
SILVER_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/maintenance_schedules/"

s3 = boto3.client("s3")

# ---------------------------
# CONSTANTS & ALLOWED LISTS
# ---------------------------
VALID_SERVICE_TYPES = [
    "Engine Overhaul", "Tire Rotation", "Oil Change", "Brake Inspection",
    "Transmission Service", "Battery Replacement", "Air Filter Replacement",
    "Coolant Flush", "Wheel Alignment", "Full Vehicle Inspection",
    "Exhaust System Check", "Fuel System Cleaning", "Suspension Check",
    "AC Service", "Electrical System Audit"
]

PUBLIC_HOLIDAYS = [
    "2024-01-26", "2024-08-15", "2024-10-02", "2024-12-25",
    "2025-01-26", "2025-08-15", "2025-10-02", "2025-12-25",
    "2026-01-26"
]

# ---------------------------
# HELPER: CHECK IF PROCESSED
# ---------------------------
def is_already_processed(filename, etag):
    """
    Checks if a marker file exists in the processed folder.
    Marker name includes ETag to detect if content has changed.
    """
    clean_etag = etag.replace('"', '')
    marker_key = f"{PROCESSED_PREFIX}{filename}.{clean_etag}.done"
    try:
        s3.head_object(Bucket=BUCKET, Key=marker_key)
        return True 
    except:
        return False 

# ---------------------------
# LIST FILES
# ---------------------------
files_to_process = []
paginator = s3.get_paginator("list_objects_v2")

print("Checking for new maintenance schedule files...")
for page in paginator.paginate(Bucket=BUCKET, Prefix=RAW_PREFIX):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            filename = key[len(RAW_PREFIX):]
            if not is_already_processed(filename, obj["ETag"]):
                files_to_process.append({
                    "key": key,
                    "filename": filename,
                    "etag": obj["ETag"].replace('"', '')
                })
            else:
                print(f"Skipping (Already Processed): {filename}")

print(f"Total files to process: {len(files_to_process)}")

# ---------------------------
# MAIN PROCESSING LOOP
# ---------------------------
for file_info in files_to_process:
    file_key = file_info["key"]
    filename = file_info["filename"]
    file_etag = file_info["etag"]
    
    file_path = f"s3://{BUCKET}/{file_key}"
    print(f"Processing file: {file_path}")

    try:
        # READ DATA
        df = spark.read.option("header", True).csv(file_path)

        # 1. NORMALIZATION & NULL HANDLING
        NULL_SENTINELS = ["", "null", "n/a", "none", "nan"]
        for c in df.columns:
            df = df.withColumn(c, trim(col(c)))
            df = df.withColumn(
                c,
                when(lower(col(c)).isin(NULL_SENTINELS) | col(c).isNull(), None).otherwise(col(c))
            )

        # 2. VIN VALIDATION
        df = df.withColumn("vin_upper", upper(col("vin")))
        vin_valid_cond = (col("vin_upper").isNotNull()) & (length(col("vin_upper")) == 8) & (col("vin_upper").rlike("^[A-Z0-9]{8}$")) & (col("vin_upper") != "00000000")

        # 3. DATE CLEANING & PARSING
        # Strip time if it exists (e.g., 2024-04-28 10:00:00 -> 2024-04-28)
        df = df.withColumn("date_str", substring(col("service_date"), 1, 10))
        df = df.withColumn("service_date_parsed", to_date(col("date_str"), "yyyy-MM-dd"))
        
        # Date flags
        df = df.withColumn("is_weekend", dayofweek(col("service_date_parsed")).isin([1, 7]))
        df = df.withColumn("is_holiday", date_format(col("service_date_parsed"), "yyyy-MM-dd").isin(PUBLIC_HOLIDAYS))

        date_valid_cond = (col("service_date_parsed").isNotNull()) & (year(col("service_date_parsed")) >= 2018) & (year(col("service_date_parsed")) <= 2029)

        # 4. SERVICE TYPE STANDARDIZATION
        # Convert to Title Case for matching, then to Upper Underscore for output
        df = df.withColumn("service_title", initcap(regexp_replace(col("service_type"), "[^A-Za-z0-9 ]", "")))
        df = df.withColumn("service_title", trim(regexp_replace(col("service_title"), " +", " ")))
        
        service_valid_cond = (col("service_type").isNotNull()) & (col("service_title").isin(VALID_SERVICE_TYPES)) & (~col("service_type").rlike("(?i)(drop|select|script)"))

        # 5. CORRUPTION FLAGS
        def flag(cond, msg):
            return when(cond, msg).otherwise("")

        reason = concat(
            flag(~vin_valid_cond, "Invalid VIN | "),
            flag(~date_valid_cond, "Invalid Service Date | "),
            flag(~service_valid_cond, "Invalid Service Type | ")
        )
        df = df.withColumn("corruption_reason", regexp_replace(reason, r"\s*\|\s*$", ""))

        # 6. DEDUPLICATION (Within file)
        # Drop exact duplicates of VIN, Date, and standardized Type
        df = df.dropDuplicates(["vin_upper", "service_date_parsed", "service_title"])

        # 7. BUSINESS RULE: MAX 12 SERVICES PER YEAR
        win_annual = Window.partitionBy("vin_upper", year(col("service_date_parsed")))
        df = df.withColumn("annual_count", count("*").over(win_annual))
        
        df = df.withColumn("corruption_reason", 
            when(col("annual_count") > 12, 
                 when(col("corruption_reason") == "", "Exceeds 12 services/year").otherwise(concat(col("corruption_reason"), lit(" | Exceeds 12 services/year"))))
            .otherwise(col("corruption_reason")))

        # 8. SPLIT AND WRITE
        clean_df = df.filter(col("corruption_reason") == "")
        corrupt_df = df.filter(col("corruption_reason") != "")

        if not clean_df.rdd.isEmpty():
            final_clean = clean_df.select(
                col("vin_upper").alias("vin"),
                col("service_date_parsed").alias("service_date"),
                regexp_replace(upper(col("service_title")), " ", "_").alias("service_type"),
                "is_weekend",
                "is_holiday",
                current_timestamp().alias("ingestion_time")
            )
            # Partition by service_type as per your original requirement
            final_clean.write.mode("append").partitionBy("service_type").parquet(SILVER_PATH)

        if not corrupt_df.rdd.isEmpty():
            final_corrupt = corrupt_df.select("vin", "service_date", "service_type", "corruption_reason")
            final_corrupt.write.mode("append").option("header", True).csv(f"s3://{BUCKET}/{CORRUPT_PREFIX}")

        # 9. CREATE MARKER FILE
        marker_key = f"{PROCESSED_PREFIX}{filename}.{file_etag}.done"
        s3.put_object(
            Bucket=BUCKET, 
            Key=marker_key, 
            Body=f"Processed on {datetime.now().isoformat()}"
        )
        print(f"Finished Processing: {filename}")

    except Exception as e:
        print(f"Error on {file_key}: {str(e)}")

print("Glue Job Execution Finished Successfully")