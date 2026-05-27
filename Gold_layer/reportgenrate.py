from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("Driver_Safety_Report_Generator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# INPUT PATH (YOUR ACTUAL PATH)
# ============================================================
INPUT_PATH = "s3://ttn-de-bootcamp-gold-us-east-1/deepak-gold-07-04/driver_safety_status/"

# ============================================================
# READ DATA
# ============================================================
df = spark.read.parquet(INPUT_PATH)

print("✅ Data Loaded")
print("Row Count:", df.count())
df.show(5, False)

# ============================================================
# FORMAT REPORT TEXT
# ============================================================
report_df = df.withColumn(
    "report_text",
    concat(
        lit("Driver ID: "),             coalesce(col("driver_id").cast("string"),                        lit("N/A")), lit("\n"),
        lit("Month: "),                 coalesce(col("month").cast("string"),                             lit("N/A")), lit("\n"),
        lit("Strike Count: "),          coalesce(col("strike_count").cast("string"),                      lit("0")),   lit("\n"),
        lit("Current Adjusted Rate: "), coalesce(round(col("current_adjusted_rate"), 2).cast("string"),   lit("0"))
    )
)

# ============================================================
# COLLECT TO DRIVER
# ============================================================
rows = report_df.select("driver_id", "month", "report_text").collect()

print(f"✅ Rows collected: {len(rows)}")

# ============================================================
# SAFETY CHECK
# ============================================================
if len(rows) == 0:
    print("❌ No data to write")
    spark.stop()
    exit()

# ============================================================
# S3 CLIENT
# ============================================================
s3 = boto3.client("s3")

bucket = "ttn-de-bootcamp-gold-us-east-1"

# ============================================================
# WRITE FILES
# ============================================================
for row in rows:
    driver_id = row["driver_id"]
    month = row["month"]
    text = row["report_text"]

    key = f"deepak-gold-07-04/flagged_report/{month}/{driver_id}.txt"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8")
    )

    print(f"✔ Written: {key}")

print("🎉 All reports generated successfully")

# ============================================================
# STOP
# ============================================================
spark.stop()
