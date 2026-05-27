from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("Driver_Safety_Final_Pipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n🔥 JOB STARTED\n", flush=True)


# ============================================================
# Postgres initalistiom
# ============================================================


postgres_url = "jdbc:postgresql://44.208.24.54:5432/omniroutedb"

properties = {
    "user": "harsh",
    "password": "harsh",
    "driver": "org.postgresql.Driver"
}

try:

    # ============================================================
    # STEP 1: READ SILVER DATA
    # ============================================================
    print("📥 STEP 1: Reading Silver Data...", flush=True)

    telemetry_df = spark.read \
        .option("recursiveFileLookup", "true") \
        .parquet(
            "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/telemetry/clean_data/"
        )

    assignment_df = spark.read.parquet(
        "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/vehicle_assignment/"
    )

    zones_df = spark.read.parquet(
        "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/restricted_zones/"
    )

    print("✅ Telemetry Count:", telemetry_df.count())
    telemetry_df.show(15, truncate=False)

    print("✅ Assignment Count:", assignment_df.count())
    assignment_df.show(15, truncate=False)

    print("✅ Zones Count:", zones_df.count())
    zones_df.show(15, truncate=False)

    # ============================================================
    # STEP 2: CLEAN DATA
    # ============================================================
    print("\n🧹 STEP 2: Cleaning Data...", flush=True)

    telemetry_df = telemetry_df \
        .withColumn(
            "event_timestamp",
            to_timestamp("event_timestamp")
        ) \
        .withColumn(
            "month",
            date_format("event_timestamp", "yyyy-MM")
        )

    assignment_df = assignment_df \
        .withColumn(
            "start_date",
            to_timestamp("start_date")
        ) \
        .withColumn(
            "end_date",
            to_timestamp("end_date")
        ) \
        .filter(col("start_date").isNotNull())

    zones_df = zones_df.filter(
        col("zone_name") != "Zone_T"
    )

    print("✅ Cleaning Completed")

    # ============================================================
    # STEP 3: GET ACTIVE ASSIGNMENTS
    # ============================================================
    print("\n🚛 STEP 3: Getting Active Assignments...", flush=True)

    active_assignment_df = assignment_df.filter(
        col("status") == "IN-TRANSIT"
    )

    assignment_window = Window.partitionBy("vin").orderBy(
        col("start_date").desc()
    )

    latest_assignment_df = active_assignment_df \
        .withColumn(
            "rn",
            row_number().over(assignment_window)
        ) \
        .filter(col("rn") == 1) \
        .drop("rn")

    print("✅ Active Assignment Records")
    latest_assignment_df.show(15, truncate=False)

    # ============================================================
    # STEP 4: JOIN TELEMETRY + ASSIGNMENT
    # ============================================================
    print("\n🔗 STEP 4: Joining telemetry + assignments...", flush=True)

    joined_df = telemetry_df.alias("t").join(
        latest_assignment_df.alias("a"),
        col("t.vin") == col("a.vin"),
        "left"
    ).select(
        col("t.driver_id"),
        col("t.vin"),
        col("t.event_timestamp"),
        col("t.month"),
        col("t.speed"),
        col("t.lat"),
        col("t.long"),
        col("a.daily_rate")
    )

    print("✅ Join Complete")
    joined_df.show(15, truncate=False)

    # ============================================================
    # STEP 5: JOIN RESTRICTED ZONES
    # ============================================================
    print("\n📍 STEP 5: Joining Restricted Zones...", flush=True)

    zone_df = joined_df.alias("j").join(
        zones_df.alias("z"),
        (
            (col("j.lat") >= col("z.min_lat")) &
            (col("j.lat") <= col("z.max_lat")) &
            (col("j.long") >= col("z.min_long")) &
            (col("j.long") <= col("z.max_long"))
        ),
        "left"
    ).select(
        "j.*",
        "z.zone_name"
    )

    print("✅ Zone Join Complete")
    zone_df.show(15, truncate=False)

    # ============================================================
    # STEP 6: CREATE VIOLATION FLAGS
    # ============================================================
    print("\n🚨 STEP 6: Creating Violation Flags...", flush=True)

    final_df = zone_df \
        .withColumn(
            "speed_violation_flag",
            when(col("speed") > 110, 1).otherwise(0)
        ) \
        .withColumn(
            "zone_violation_flag",
            when(col("zone_name").isNotNull(), 1).otherwise(0)
        ) \
        .withColumn(
            "strike_flag",
            when(
                (col("speed") > 110) |
                (col("zone_name").isNotNull()),
                1
            ).otherwise(0)
        ) \
        .withColumn(
            "violation_date",
            to_date("event_timestamp")
        ) \
        .withColumn(
            "ingestion_time",
            current_timestamp()
        )

    print("✅ Final Violations Data")
    final_df.show(15, truncate=False)


    # ============================================================
    # GOLD TABLE 1: VIOLATION EVENTS
    # ============================================================
    print("\n🚨 Writing violation_events...", flush=True)

    violation_events_df = final_df.select(
        "driver_id",
        "vin",
        "event_timestamp",
        "violation_date",
        "month",
        "speed",
        "lat",
        "long",
        "speed_violation_flag",
        "zone_violation_flag",
        "strike_flag",
        "zone_name",
        "ingestion_time"
    )

    violation_events_df.write \
        .mode("overwrite") \
        .partitionBy("month") \
        .parquet(
            "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/violation_events/"
        )
    
    # Postgres write
    violation_events_df.write.mode("overwrite").jdbc(
        url=postgres_url,
        table="violation_events",
        properties=properties
        )

    print("✅ violation_events written")

except Exception as e:
    print("\n❌ ERROR:", str(e), flush=True)
    import traceback
    traceback.print_exc()

print("\n🔥 JOB COMPLETED\n")
sys.stdout.flush()
