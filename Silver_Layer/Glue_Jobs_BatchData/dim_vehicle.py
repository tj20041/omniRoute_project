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
RAW_PREFIX = "deepak-bronze-07-04/vehicle_registry/raw_files/"
PROCESSED_PREFIX = "deepak-bronze-07-04/vehicle_registry/processed/"
CORRUPT_PREFIX = "deepak-bronze-07-04/vehicle_registry/corrupted_files/"

# Destination path for Silver Layer
SILVER_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/vehicle_registry/"

# Validation Constants
VALID_FUEL_TYPES = ["CNG", "LNG", "Petrol", "Diesel", "Electric", "Hybrid"]
MIN_YEAR = 2000
MAX_YEAR = datetime.now().year
MAX_KMPL = 50.0

s3 = boto3.client("s3")

# ---------------------------
# HELPER: CHECK IF PROCESSED
# ---------------------------
def is_already_processed(filename, etag):
    """
    Checks if a marker file exists in the processed folder.
    The marker name includes the ETag (content hash) to detect data changes.
    """
    clean_etag = etag.replace('"', '')
    marker_key = f"{PROCESSED_PREFIX}{filename}.{clean_etag}.done"
    
    try:
        s3.head_object(Bucket=BUCKET, Key=marker_key)
        return True 
    except:
        return False 

# ---------------------------
# LIST FILES (PAGINATION SAFE)
# ---------------------------
files_to_process = []
paginator = s3.get_paginator("list_objects_v2")

print("Checking for new or updated files...")

for page in paginator.paginate(Bucket=BUCKET, Prefix=RAW_PREFIX):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            filename = key[len(RAW_PREFIX):]
            etag = obj["ETag"]
            
            if not is_already_processed(filename, etag):
                files_to_process.append({
                    "key": key,
                    "filename": filename,
                    "etag": etag.replace('"', '')
                })
            else:
                # This log confirms the file was identified as already processed
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
    print(f"Processing: {file_path}")

    try:
        # 1. READ RAW DATA
        raw_df = spark.read.option("header", True).csv(file_path)
        raw_df = raw_df.toDF(*[c.strip().lower() for c in raw_df.columns])

        # 2. STANDARDIZATION & INITIAL CLEANING
        NULL_LIKE = ["", "null", "NULL", "N/A", "nan"]
        for c in raw_df.columns:
            raw_df = raw_df.withColumn(c, trim(col(c)))
            raw_df = raw_df.withColumn(c, when(lower(col(c)).isin([x.lower() for x in NULL_LIKE]), None).otherwise(col(c)))

        raw_df = raw_df.withColumn("lookup_model", lower(col("model")))
        raw_df = raw_df.withColumn("lookup_fuel", initcap(col("fuel_type")))
        raw_df = raw_df.withColumn("base_kmpl", regexp_replace("base_kmpl", "[^0-9.]", "").cast("double"))

        # 3. BUILD ROBUST LOOKUP TABLE FOR INFERENCE (keyed on model + fuel_type)
        #    Pull known valid KMPL values from the current file
        current_valid = raw_df.select(
            col("lookup_model").alias("model"),
            col("lookup_fuel").alias("fuel_type"),
            "base_kmpl"
        ).filter(col("base_kmpl").isNotNull())

        #    Also pull from the silver layer (historical data) for richer coverage
        try:
            silver_valid = spark.read.parquet(SILVER_PATH).select(
                lower(col("model")).alias("model"),
                initcap(col("fuel_type")).alias("fuel_type"),
                "base_kmpl"
            ).filter(col("base_kmpl").isNotNull())
        except Exception:
            silver_valid = spark.createDataFrame([], "model string, fuel_type string, base_kmpl double")

        #    Combine both sources and pick the MOST FREQUENT (mode) KMPL per (model, fuel_type)
        #    If 10 VINs say base_kmpl=14.0 and 1 says 12.5, the authoritative value is 14.0.
        #    In case of a tie in frequency, the higher KMPL is picked as a tiebreaker.
        combined_ref = silver_valid.union(current_valid)
        freq_ref = combined_ref.groupBy("model", "fuel_type", "base_kmpl").agg(
            count("*").alias("freq")
        )
        win_lookup = Window.partitionBy("model", "fuel_type").orderBy(
            col("freq").desc(),        # most frequent first
            col("base_kmpl").desc()    # tiebreaker: higher value
        )
        master_lookup = (
            freq_ref
            .withColumn("rn", row_number().over(win_lookup))
            .filter(col("rn") == 1)
            .drop("rn", "freq")
        )

        # 4. INFER & STANDARDIZE KMPL
        #    Step A: Fill NULLs from the lookup (model + fuel_type match)
        #    Step B: For records that had a non-NULL value but it doesn't match the
        #            authoritative max, override it — this ensures consistency across
        #            all records of the same (model, fuel_type) pair
        df = raw_df.alias("r").join(
            master_lookup.alias("l"),
            on=(
                (col("r.lookup_model") == col("l.model")) &
                (col("r.lookup_fuel") == col("l.fuel_type"))
            ),
            how="left"
        ).select(
            col("r.*"),
            # Use the lookup max KMPL as the authoritative value.
            # Falls back to the record's own value if no lookup match exists.
            coalesce(col("l.base_kmpl"), col("r.base_kmpl")).alias("inferred_kmpl")
        )

        # 5. VALIDATION RULES (Post-Inference)
        df = df.withColumn("_year_int", expr("try_cast(mfg_year AS INT)"))
        fuel_col = initcap(col("fuel_type"))

        def flag(cond, msg):
            return when(cond, msg).otherwise("")

        reason = concat(
            flag(col("vin").isNull(), "VIN missing | "),
            flag(length(col("vin")) != 8, "VIN length error | "),
            flag(col("model").isNull(), "Model missing | "),
            flag(col("inferred_kmpl").isNull(), "KMPL not found in history/file | "),
            flag(col("_year_int").isNull() | (col("_year_int") < MIN_YEAR) | (col("_year_int") > MAX_YEAR), "Year range error | "),
            flag((col("inferred_kmpl") <= 0) | (col("inferred_kmpl") > MAX_KMPL), "Invalid KMPL range | "),
            flag(~fuel_col.isin(VALID_FUEL_TYPES), "Unsupported Fuel Type | "),
            flag(col("model").rlike("^[0-9]+$"), "Model is numeric | "),
            flag(col("model").rlike("[^\x00-\x7F]"), "Model contains non-ASCII characters | "),
            flag(lower(col("model")).isin("unknown", "n/a", "model xxxxxxxxxxxxxxxxxxxx") | (length(col("model")) > 50), "Model invalid or too long | "),
            flag(col("vin").rlike("^(.)\\1{7}$"), "VIN all same characters | "),
            flag(~col("vin").rlike("(?=.*[A-Z])(?=.*[0-9])"), "VIN must contain both letters and numbers | ")
        )
        
        df = df.withColumn("corruption_reason", regexp_replace(reason, r"\s*\|\s*$", ""))

        # 6. DEDUPLICATION (By VIN)
        dedup_window = Window.partitionBy("vin").orderBy(lit(1))
        df = df.withColumn("row_num", row_number().over(dedup_window))
        
        duplicates = df.filter(col("row_num") > 1).withColumn("corruption_reason", lit("Duplicate VIN in file"))
        df = df.filter(col("row_num") == 1).drop("row_num")

        # 7. SPLIT AND WRITE
        clean_df = df.filter(col("corruption_reason") == "")
        corrupt_df = df.filter(col("corruption_reason") != "").unionByName(duplicates, allowMissingColumns=True)

        if not clean_df.rdd.isEmpty():
            clean_output = clean_df.select(
                upper(col("vin")).alias("vin"),
                initcap(col("model")).alias("model"),
                col("_year_int").alias("mfg_year"),
                initcap(col("fuel_type")).alias("fuel_type"),
                round(col("inferred_kmpl"), 2).alias("base_kmpl")
            )
            clean_output.write.mode("append").partitionBy("mfg_year").parquet(SILVER_PATH)

        if not corrupt_df.rdd.isEmpty():
            corrupt_final = corrupt_df.drop("lookup_model", "lookup_fuel", "inferred_kmpl")
            corrupt_final.write.mode("append").option("header", True).csv(f"s3://{BUCKET}/{CORRUPT_PREFIX}")

        # 8. MARK AS PROCESSED (Marker File includes ETag)
        marker_key = f"{PROCESSED_PREFIX}{filename}.{file_etag}.done"
        s3.put_object(
            Bucket=BUCKET, 
            Key=marker_key, 
            Body=f"Processed Successfully at {datetime.now().isoformat()}"
        )
        print(f"Finished: {filename}")

    except Exception as e:
        print(f"Error processing {file_key}: {str(e)}")

print("Glue Job Execution Finished")
