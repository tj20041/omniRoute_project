from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

spark = SparkSession.builder \
    .appName("Active_Fleet_Snapshot") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n🔥 JOB STARTED\n", flush=True)

postgres_url = "jdbc:postgresql://44.208.24.54:5432/omniroutedb"

properties = {
    "user": "harsh",
    "password": "harsh",
    "driver": "org.postgresql.Driver"
}

try:

    # ============================================================
    # READ DATA
    # ============================================================
    vehicle_registry = spark.read.parquet(
        "s3://ttn-de-bootcamp-silver-us-east-1/deepak-silver-07-04/vehicle_registry/"
    )

    vehicle_driver_history = spark.read.parquet(
        "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/fact_asset_history/"
    )

    # ============================================================
    # ACTIVE VEHICLES (end_date NULL)
    # ============================================================
    active_df = vehicle_driver_history.filter(col("end_date").isNull())

    # ============================================================
    # JOIN
    # ============================================================
    joined_df = active_df.join(vehicle_registry, "vin", "inner")

    # ============================================================
    # AGGREGATION
    # ============================================================
    final_df = joined_df.groupBy("model").agg(
        count("vin").alias("no_of_active_vehicles")
    ).withColumn(
        "snapshot_time", current_timestamp()
    )

    final_df = final_df.withColumn(
        "snapshot_date",
        to_date(col("snapshot_time"))
    )

    # ============================================================
    # WRITE
    # ============================================================
    final_df.write \
        .mode("overwrite") \
        .partitionBy("snapshot_date") \
        .parquet(
            "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/active_fleet_snapshot/"
        )

    final_df.write.mode("overwrite").jdbc(
        url=postgres_url,
        table="active_fleet_snapshot",
        properties=properties
    )

    print("✅ active_fleet_snapshot written")

except Exception as e:
    print("❌ ERROR:", str(e))
    import traceback
    traceback.print_exc()

spark.stop()
