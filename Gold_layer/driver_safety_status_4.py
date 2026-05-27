from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("Driver_Safety_Final_Pipeline") \
    .config("spark.sql.ansi.enabled", "false") \
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

# ============================================================
# ARCHITECTURE — TWO INDEPENDENT TRACKS
# ─────────────────────────────────────────────────────────────
# TRACK A — violation_events (gold):
#   driver_id, strike_flag, speed_violation_flag,
#   zone_violation_flag, violation_date, month
#   → strike counts, suspension logic, deduction trigger dates
#
# TRACK B — vehicle_assignment (silver):
#   driver_id, start_date, end_date, daily_rate
#   → base_rate only (daily_rate × days active in month)
#
# The two tracks NEVER join for strike counting.
# They only meet in the final monthly aggregation (Step 10)
# where daily deductions are applied.
# ============================================================

try:
    # ============================================================
    # STEP 1: READ
    # ============================================================
    print("📥 STEP 1: Reading data...", flush=True)

    violation_df = spark.read.parquet(
        "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/violation_events/"
    )
    assignment_df = spark.read.parquet(
        "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/vehicle_assignment/"
    )

    print(f"✅ Violations : {violation_df.count()}")
    print(f"✅ Assignments: {assignment_df.count()}")

    # ============================================================
    # STEP 2: CAST TYPES
    # ============================================================
    print("\n🧹 STEP 2: Casting types...", flush=True)

    # TRACK A — violations
    violation_df = violation_df \
        .withColumn("event_timestamp",      to_timestamp("event_timestamp")) \
        .withColumn("month",                date_format("event_timestamp", "yyyy-MM")) \
        .withColumn("violation_date",       to_date("event_timestamp")) \
        .withColumn("strike_flag",          col("strike_flag").cast("int")) \
        .withColumn("speed_violation_flag", col("speed_violation_flag").cast("int")) \
        .withColumn("zone_violation_flag",  col("zone_violation_flag").cast("int")) \
        .filter(col("driver_id").isNotNull())

    # TRACK B — assignment (silver parquet already has proper date columns)
    assignment_df = assignment_df \
        .withColumn("start_date", to_timestamp("start_date")) \
        .withColumn("end_date",   to_timestamp("end_date")) \
        .withColumn("daily_rate", col("daily_rate").cast("double")) \
        .filter(col("start_date").isNotNull()) \
        .filter(col("driver_id").isNotNull())

    print("✅ Types cast")

    # ============================================================
    # TRACK A — STEP 3: DAILY STRIKES FROM VIOLATION EVENTS
    # One row per (driver_id, month, violation_date).
    # This is the ONLY place strikes are counted.
    # Assignment table is never used for strike counting.
    # ============================================================
    print("\n📊 STEP 3 [TRACK A]: Daily strikes from violations...", flush=True)

    daily_strikes = violation_df.groupBy(
        "driver_id", "month", "violation_date"
    ).agg(
        sum("strike_flag").alias("daily_strike_count"),
        sum("speed_violation_flag").alias("speed_violations"),
        sum("zone_violation_flag").alias("zone_violations")
    )

    # ============================================================
    # TRACK A — STEP 4: CUMULATIVE STRIKE COUNT PER DRIVER PER MONTH
    # Running sum over days within each (driver_id, month).
    # Aggregated to daily level first (Step 3) so same-day events
    # don't produce non-deterministic per-event running counts.
    # ============================================================
    print("\n🔢 STEP 4 [TRACK A]: Running strike count...", flush=True)

    strike_window = Window.partitionBy("driver_id", "month") \
        .orderBy("violation_date") \
        .rowsBetween(Window.unboundedPreceding, 0)

    running_df = daily_strikes \
        .withColumn("running_strike_count",
                    sum("daily_strike_count").over(strike_window))

    # ============================================================
    # TRACK A — STEP 5: FIND PERMANENT SUSPENSION POINT
    # Once running_strike_count hits 10 in any month, the driver
    # is permanently suspended from that date forward.
    # min() used to get the earliest suspension date globally.
    # ============================================================
    print("\n🚫 STEP 5 [TRACK A]: Finding suspension points...", flush=True)

    suspension_df = running_df \
        .filter(col("running_strike_count") >= 10) \
        .groupBy("driver_id") \
        .agg(
            min("violation_date").alias("suspended_on"),
            min("month").alias("suspended_month")
        )

    print(f"✅ Suspended drivers: {suspension_df.count()}")
    suspension_df.show(20, truncate=False)

    # ============================================================
    # TRACK A — STEP 6: FILTER STRIKES UP TO SUSPENSION DATE
    # For suspended drivers: only keep violation days up to and
    # including the suspension date. Beyond that, data is ignored.
    # For non-suspended drivers: keep all.
    # ============================================================
    print("\n✂️  STEP 6 [TRACK A]: Filtering to suspension date...", flush=True)

    valid_strikes = running_df.join(
        suspension_df.select("driver_id", "suspended_on"),
        "driver_id", "left"
    ).filter(
        col("suspended_on").isNull() |
        (col("violation_date") <= col("suspended_on"))
    ).select(
        "driver_id", "month", "violation_date",
        "daily_strike_count", "speed_violations", "zone_violations"
    )

    # Monthly strike totals — used in final output for strike_count column
    monthly_strikes = valid_strikes.groupBy("driver_id", "month").agg(
        sum("daily_strike_count").alias("strike_count"),
        sum("speed_violations").alias("speed_violations"),
        sum("zone_violations").alias("zone_violations")
    )

    print(f"✅ Valid strike rows: {valid_strikes.count()}")

    # ============================================================
    # TRACK B — STEP 7: EXPAND ASSIGNMENT TO DAILY ROWS
    # Each assignment record generates one row per calendar day.
    # For suspended drivers, earnings stop at suspended_on date.
    # base_rate = sum(daily_rate × days) per (driver_id, month).
    # ============================================================
    print("\n📅 STEP 7 [TRACK B]: Expanding assignments to daily rows...", flush=True)

    assignment_intervals = assignment_df.join(
        suspension_df.select("driver_id", "suspended_on"),
        "driver_id", "left"
    ).withColumn(
        "effective_end",
        when(
            col("suspended_on").isNotNull(),
            least(
                coalesce(to_date(col("end_date")), current_date()),
                col("suspended_on")
            )
        ).otherwise(
            coalesce(to_date(col("end_date")), current_date())
        )
    ).withColumn(
        "effective_start", to_date(col("start_date"))
    ).withColumn(
        "num_days", datediff(col("effective_end"), col("effective_start"))
    ).filter(
        (col("num_days") > 0) & (col("num_days") < 3650)
    ).withColumn(
        "day_offset", explode(sequence(lit(0), col("num_days") - 1))
    ).withColumn(
        "driven_date", expr("date_add(effective_start, day_offset)")
    ).withColumn(
        "month", date_format(col("driven_date"), "yyyy-MM")
    ).select(
        "driver_id", "daily_rate", "driven_date", "month"
    )

    # One row per (driver_id, driven_date) — sum across all VINs that day
    daily_earnings = assignment_intervals.groupBy(
        "driver_id", "driven_date", "month"
    ).agg(
        sum("daily_rate").alias("daily_rate")
    )

    # Monthly base rate = sum of all daily_rates in the month
    monthly_base = daily_earnings.groupBy("driver_id", "month").agg(
        sum("daily_rate").alias("base_rate")
    )

    print(f"✅ Daily earnings rows: {daily_earnings.count()}")

    # ============================================================
    # STEP 8: APPLY DAILY DEDUCTION
    # Join daily_earnings (TRACK B) with valid_strikes (TRACK A)
    # on (driver_id, month, date) to compute per-day deduction.
    # Deduction = daily_rate × daily_strike_count × 5%, capped at daily_rate.
    # Days with no violations earn full daily_rate (deduction = 0).
    # ============================================================
    print("\n📉 STEP 8: Applying daily deductions...", flush=True)

    daily_df = daily_earnings.alias("de").join(
        valid_strikes.alias("vs"),
        (col("de.driver_id")  == col("vs.driver_id")) &
        (col("de.month")       == col("vs.month")) &
        (col("de.driven_date") == col("vs.violation_date")),
        "left"
    ).select(
        col("de.driver_id"),
        col("de.month"),
        col("de.driven_date"),
        col("de.daily_rate"),
        coalesce(col("vs.daily_strike_count"), lit(0)).alias("daily_strike_count")
    ).withColumn(
        "daily_deduction",
        when(
            col("daily_strike_count") > 0,
            least(
                col("daily_rate"),
                col("daily_rate") * col("daily_strike_count") * 0.05
            )
        ).otherwise(lit(0.0))
    ).withColumn(
        "daily_earning", col("daily_rate") - col("daily_deduction")
    )

    monthly_deductions = daily_df.groupBy("driver_id", "month").agg(
        round(sum("daily_deduction"), 2).alias("total_deduction_amount"),
        round(sum("daily_earning"),   2).alias("current_adjusted_rate")
    )

    # ============================================================
    # STEP 9: ASSEMBLE FINAL RESULT
    # Join the three monthly aggregations:
    #   - monthly_strikes  (TRACK A) → strike_count, speed/zone violations
    #   - monthly_base     (TRACK B) → base_rate
    #   - monthly_deductions         → deduction + adjusted rate
    # Drivers with earnings but no strikes get strike_count = 0.
    # Drivers with strikes but no earnings still appear (left join base).
    # ============================================================
    print("\n📦 STEP 9: Assembling final monthly result...", flush=True)

    result_df = monthly_base \
        .join(monthly_strikes,    ["driver_id", "month"], "left") \
        .join(monthly_deductions, ["driver_id", "month"], "left") \
        .withColumn("strike_count",
            coalesce(col("strike_count"), lit(0))) \
        .withColumn("speed_violations",
            coalesce(col("speed_violations"), lit(0))) \
        .withColumn("zone_violations",
            coalesce(col("zone_violations"), lit(0))) \
        .withColumn("total_deduction_amount",
            coalesce(col("total_deduction_amount"), lit(0.0))) \
        .withColumn("current_adjusted_rate",
            coalesce(col("current_adjusted_rate"), col("base_rate"))) \
        .withColumn("base_rate", round(col("base_rate"), 2))

    # ============================================================
    # STEP 10: STATUS
    # SUSPENDED = driver hit 10+ strikes in any month.
    # All months from suspended_month onward are marked SUSPENDED.
    # Records are KEPT — not deleted.
    # ============================================================
    print("\n🏷️  STEP 10: Setting status...", flush=True)

    result_df = result_df.join(
        suspension_df.select("driver_id", "suspended_month"),
        "driver_id", "left"
    ).withColumn(
        "status",
        when(
            col("suspended_month").isNotNull() &
            (col("month") >= col("suspended_month")),
            lit("SUSPENDED")
        ).otherwise(lit("ACTIVE"))
    ).drop("suspended_month")

    final_count = result_df.count()
    print(f"\n✅ Final records : {final_count}")
    print(f"   SUSPENDED     : {result_df.filter(col('status')=='SUSPENDED').count()}")
    print(f"   ACTIVE        : {result_df.filter(col('status')=='ACTIVE').count()}")
    result_df.orderBy("driver_id", "month").show(20, truncate=False)

    # ============================================================
    # WRITE OUTPUT
    # ============================================================
    print("\n💾 Writing output...", flush=True)

    result_df.write \
        .mode("overwrite") \
        .partitionBy("month") \
        .parquet(
            "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/driver_safety_status/"
        )

    result_df.write.mode("overwrite").jdbc(
        url=postgres_url,
        table="driver_safety_status",
        properties=properties
    )

    print("✅ DRIVER SAFETY PIPELINE COMPLETED")

except Exception as e:
    print("\n❌ ERROR:", str(e))
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n🔥 JOB COMPLETED\n")
sys.stdout.flush()
