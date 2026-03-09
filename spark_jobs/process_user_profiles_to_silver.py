"""
process_user_profiles_to_silver.py
───────────────────────────────────
Reads raw user_profiles JSONLine from GCS and writes directly to Silver.
No Bronze layer — data quality is guaranteed by the supplier (perfect quality).

This pipeline is triggered manually, not on a schedule.

Fields in source:
  email, full_name, state, birth_date, phone_number

Transformations:
  - Split full_name → first_name, last_name
  - Cast birth_date to DateType
  - Drop duplicates by email (email is the join key used downstream)
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── 1. Argument parsing ─────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--raw_input",     required=False,
                    default="gs://robot-dream-course-data-lake/raw/user_profiles/")
parser.add_argument("--silver_output", required=False,
                    default="gs://robot-dream-course-data-lake/silver/user_profiles/")
args = parser.parse_args()

# ── 2. Spark session ────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("process_user_profiles_to_silver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── 3. Read raw JSONLine ─────────────────────────────────────────────────────
#       Spark infers schema from JSON — all fields are clean per supplier guarantee.
raw_df = spark.read.json(args.raw_input)

raw_count = raw_df.count()
print(f"📥 Raw user_profiles rows read: {raw_count}")

# ── 4. Split full_name into first_name and last_name ────────────────────────
#       "Jorge Sullivan" → first_name="Jorge", last_name="Sullivan"
#       For single-word names, last_name will be null.
silver_df = (
    raw_df
    .withColumn("name_parts", F.split(F.col("full_name"), " ", 2))
    .withColumn("first_name", F.col("name_parts").getItem(0))
    .withColumn("last_name",  F.col("name_parts").getItem(1))
    .drop("name_parts", "full_name")
)

# ── 5. Cast types ────────────────────────────────────────────────────────────
silver_df = (
    silver_df
    .withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))
)

# ── 6. Deduplicate by email (join key used in enrich pipeline) ───────────────
silver_df = silver_df.dropDuplicates(["email"])

# ── 7. Add metadata ─────────────────────────────────────────────────────────
silver_df = silver_df.withColumn("processed_at", F.current_timestamp())

silver_count = silver_df.count()
print(f"📦 Silver user_profiles rows written: {silver_count}")

# ── 8. Write Silver Parquet ─────────────────────────────────────────────────
(
    silver_df.write
    .mode("overwrite")
    .parquet(args.silver_output)
)
print(f"✅ Silver written to: {args.silver_output}")

spark.stop()
