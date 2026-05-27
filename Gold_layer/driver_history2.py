from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("Driver_Asset_History_Pipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("\n🔥 JOB STARTED\n", flush=True)

# ============================================================
# POSTGRES CONFIG
# ============================================================
postgres_url = "jdbc:postgresql://44.208.24.54:5432/omniroutedb"

properties = {
    "user": "harsh",
    "password": "harsh",
    "driver": "org.postgresql.Driver"
}

try:
    # ============================================================
    # STEP 1: READ DATA
    # ============================================================
    print("📥 STEP 1: Reading Data...", flush=True)

    assignment_df = spark.read.parquet(
        "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/vehicle_assignment/"
    )

    safety_status_df = spark.read.parquet(
        "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/driver_safety_status/"
    )

    print(f"✅ Assignments    : {assignment_df.count()}")
    print(f"✅ Safety Status  : {safety_status_df.count()}")

    # ============================================================
    # STEP 2: CAST TYPES
    # ============================================================
    print("\n🧹 STEP 2: Casting types...", flush=True)

    assignment_df = assignment_df \
        .withColumn("start_date", to_timestamp("start_date")) \
        .withColumn("end_date",   to_timestamp("end_date")) \
        .filter(col("start_date").isNotNull())

    print("✅ Types cast")

    # ============================================================
    # STEP 3: GET SUSPENDED DRIVER LIST FROM SAFETY STATUS
    # ─────────────────────────────────────────────────────────────
    # driver_safety_status already has status = SUSPENDED for
    # drivers who hit 10+ strikes. Just read directly from there —
    # no need to recompute from violation_events.
    # ============================================================
    print("\n🚫 STEP 3: Reading suspended drivers from safety status...", flush=True)

    suspended_drivers = safety_status_df \
        .filter(col("status") == "SUSPENDED") \
        .select("driver_id") \
        .distinct()

    print(f"✅ Suspended drivers: {suspended_drivers.count()}")
    suspended_drivers.show(truncate=False)

    # ============================================================
    # STEP 4: CLOSE ALL IN-TRANSIT ASSIGNMENTS FOR SUSPENDED DRIVERS
    # ─────────────────────────────────────────────────────────────
    # For any IN-TRANSIT assignment of a suspended driver:
    #   - end_date = current processing timestamp
    #   - status   = ARCHIVED
    # All other assignments are left untouched.
    # ============================================================
    print("\n🔄 STEP 4: Closing suspended driver assignments...", flush=True)

    updated_assignment_df = assignment_df.alias("a").join(
        suspended_drivers.alias("s"),
        col("a.driver_id") == col("s.driver_id"),
        "left"
    ).withColumn(
        "end_date",
        when(
            col("s.driver_id").isNotNull() &
            (col("a.status") == "IN-TRANSIT"),
            current_timestamp()
        ).otherwise(col("a.end_date"))
    ).withColumn(
        "status",
        when(
            col("s.driver_id").isNotNull() &
            (col("a.status") == "IN-TRANSIT"),
            lit("ARCHIVED")
        ).otherwise(col("a.status"))
    ).select(
        col("a.vin"),
        col("a.driver_id"),
        col("a.start_date"),
        col("end_date"),
        col("a.daily_rate"),
        col("a.region"),
        col("status"),
        col("a.ingestion_time")
    )

    print(f"✅ Final records: {updated_assignment_df.count()}")
    updated_assignment_df.show(15, truncate=False)

    # ============================================================
    # STEP 5: WRITE TO S3 + POSTGRES
    # ============================================================
    print("\n💾 STEP 5: Writing outputs...", flush=True)

    updated_assignment_df.write \
        .mode("overwrite") \
        .parquet(
            "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/fact_asset_history/"
        )

    updated_assignment_df.write.mode("overwrite").jdbc(
        url=postgres_url,
        table="vehicle_driver_history",
        properties=properties
    )

    print("✅ fact_asset_history written to S3 and Postgres")

except Exception as e:
    print("\n❌ ERROR:", str(e), flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n🔥 JOB COMPLETED\n")
sys.stdout.flush()
