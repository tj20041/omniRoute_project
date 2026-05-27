from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys

# ============================================================
# SPARK SESSION (EMR)
# ============================================================
spark = SparkSession.builder \
    .appName("Fuel_Efficiency_Audit_EMR_Pipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("🚀 JOB STARTED")

# ============================================================
# SETTINGS & PATHS
# ============================================================
SILVER_BUCKET = "ttn-de-bootcamp-silver-us-east-1"
GOLD_BUCKET   = "ttn-de-bootcamp-gold-us-east-1"
BASE_PATH     = "deepak-silver-07-04"

SILVER_DIM_VEHICLE = f"s3://{SILVER_BUCKET}/{BASE_PATH}/vehicle_registry/"
SILVER_DIM_DATE    = f"s3://{SILVER_BUCKET}/{BASE_PATH}/dim_date/"
SILVER_DIM_MAINT   = f"s3://{SILVER_BUCKET}/{BASE_PATH}/maintenance_schedules/"
SILVER_FUEL        = f"s3://{SILVER_BUCKET}/{BASE_PATH}/fuel_transactions/"

GOLD_FACT_FUEL     = f"s3://{GOLD_BUCKET}/deepak-gold-07-04/fact_fuel_efficiency_audit/"

FUEL_DROP_PCT = 0.12

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
# 1. LOAD SILVER DATA
# ============================================================
print("📥 Loading Silver Data...")

dim_vehicle = spark.read.parquet(SILVER_DIM_VEHICLE)

dim_date = spark.read.option("header", "true") \
                     .option("inferSchema", "true") \
                     .csv(SILVER_DIM_DATE)

dim_maint = spark.read.parquet(SILVER_DIM_MAINT)

fuel_df = spark.read.parquet(SILVER_FUEL)

# ============================================================
# 2. PRE-FILTERING CALCULATIONS
# ============================================================
fuel_df = fuel_df.toDF(*[c.strip().lower() for c in fuel_df.columns])

w_odo = Window.partitionBy("vin").orderBy("timestamp")

fuel_processed = fuel_df \
    .withColumn("vin", upper(trim(col("vin")))) \
    .withColumn("fuel_liters", col("fuel_liters").cast("double")) \
    .withColumn("odometer_reading", col("odometer_reading").cast("double")) \
    .withColumn("audit_date", to_date(col("timestamp"))) \
    .withColumn("prev_odo", lag("odometer_reading").over(w_odo)) \
    .withColumn("distance_km",
        when(col("prev_odo").isNotNull() & 
             (col("odometer_reading") > col("prev_odo")),
             col("odometer_reading") - col("prev_odo"))
        .otherwise(lit(0)))

# ============================================================
# 3. EXCLUSION LOGIC
# ============================================================
invalid_dates_general = dim_date.filter(
    (col("is_weekend") == True) | (col("is_holiday") == True)
).select(col("full_date").alias("exc_date"))

maint_dates = dim_maint.select(
    col("vin"), col("service_date").alias("exc_date")
)

final_exclusion_set = maint_dates.join(
    invalid_dates_general, "exc_date", "full"
).select(
    coalesce(maint_dates.vin, lit("ALL")).alias("ex_vin"),
    "exc_date"
).distinct()

fuel_filtered = fuel_processed.join(
    final_exclusion_set,
    (fuel_processed.audit_date == final_exclusion_set.exc_date) &
    ((final_exclusion_set.ex_vin == lit("ALL")) |
     (fuel_processed.vin == final_exclusion_set.ex_vin)),
    "left_anti"
)

# ============================================================
# 4. AGGREGATION
# ============================================================
fuel_daily = fuel_filtered.filter(col("distance_km") > 0) \
    .groupBy("vin", "audit_date") \
    .agg(
        sum("distance_km").alias("total_km"),
        sum("fuel_liters").alias("total_liters")
    ) \
    .withColumn("km_per_liter",
        round(col("total_km") / col("total_liters"), 4))

# ============================================================
# 5. FINAL FACT TABLE
# ============================================================
fact_fuel = fuel_daily \
    .join(dim_vehicle.select("vin", "model", "base_kmpl"), "vin") \
    .join(dim_date.select("full_date", "date_key"),
          fuel_daily.audit_date == dim_date.full_date) \
    .withColumn("status",
        when(col("km_per_liter") < col("base_kmpl") * (1 - FUEL_DROP_PCT),
             lit("FLAGGED"))
        .otherwise(lit("OK"))) \
    .select(
        "vin", "date_key", "model", "audit_date",
        col("km_per_liter").cast("float"),
        col("base_kmpl").cast("float").alias("baseline_kmpl"),
        "status"
    )

# ============================================================
# 6. WRITE TO S3 (GOLD)
# ============================================================
if not fact_fuel.rdd.isEmpty():
    print("💾 Writing Gold Layer...")
    fact_fuel.write.mode("overwrite").parquet(GOLD_FACT_FUEL)

    print("✅ Gold data written:", GOLD_FACT_FUEL)
    print("📊 Row Count:", fact_fuel.count())
else:
    print("⚠️ No data to write")

# ============================================================
# 7. WRITE TO POSTGRES
# ============================================================
print("📤 Writing to Postgres...")

fact_fuel.write.mode("overwrite").jdbc(
    url=postgres_url,
    table="fact_fuel_efficiency_audit",
    properties=properties
)

print("✅ Postgres write complete")

print("🎉 JOB COMPLETED")
sys.stdout.flush()
