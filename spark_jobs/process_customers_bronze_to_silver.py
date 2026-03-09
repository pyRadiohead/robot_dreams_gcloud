"""
process_customers_bronze_to_silver.py
─────────────────────────────────────
Reads Bronze customers Parquet, deduplicates, casts types, renames columns
to company convention, and writes to Silver.

Deduplication strategy:
  The supplier delivers a full table dump every day, so Bronze contains the
  same customer Id across multiple files/days. Re-delivered records are exact
  copies (the source has no update timestamp), so dropDuplicates(["Id"]) is
  used — all copies are identical, so any one is equally valid.

Null handling:
  State, FirstName, LastName are intentionally kept even when NULL — they will
  be filled in the enrich_user_profiles pipeline from the user_profiles source.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ── 1. Argument parsing ─────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--bronze_input",  required=False,
                    default="gs://robot-dream-course-data-lake/bronze/customers/")
parser.add_argument("--silver_output", required=False,
                    default="gs://robot-dream-course-data-lake/silver/customers/")
args = parser.parse_args()

# ── 2. Spark session ────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("process_customers_bronze_to_silver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── 3. Read Bronze Parquet ──────────────────────────────────────────────────
bronze_df = spark.read.parquet(args.bronze_input)

bronze_count = bronze_df.count()
print(f"📥 Bronze rows read: {bronze_count}")

# ── 4. Deduplicate — one record per customer Id ─────────────────────────────
#       The supplier re-delivers all previous rows every day, so Bronze
#       contains identical duplicates across daily batches.
#       Since re-delivered records are exact copies (no update timestamp in
#       the source schema), dropDuplicates by Id is the correct approach —
#       all duplicates carry the same data so any copy is equally valid.
deduped_df = bronze_df.dropDuplicates(["Id"])

dedup_count = deduped_df.count()
print(f"🔁 Rows after deduplication: {dedup_count}  (removed {bronze_count - dedup_count} duplicates)")

# ── 5. Cast types ────────────────────────────────────────────────────────────
typed_df = (
    deduped_df
    .withColumn("Id", F.col("Id").cast(IntegerType()))
    .withColumn("RegistrationDate", F.to_date(F.col("RegistrationDate"), "yyyy-M-d"))
    .dropna(subset=["Id"])  # Id is the primary key — rows without it are unusable
)

# ── 6. Rename columns to company convention (snake_case) ────────────────────
#       State, FirstName, LastName kept even if NULL — enriched later.
silver_df = (
    typed_df
    .withColumnRenamed("Id",               "client_id")
    .withColumnRenamed("FirstName",        "first_name")
    .withColumnRenamed("LastName",         "last_name")
    .withColumnRenamed("Email",            "email")
    .withColumnRenamed("RegistrationDate", "registration_date")
    .withColumnRenamed("State",            "state")
    .drop("ingested_at")
    .withColumn("processed_at", F.current_timestamp())
)

silver_count = silver_df.count()
print(f"📦 Silver rows written: {silver_count}")

# ── 7. Write Silver Parquet ─────────────────────────────────────────────────
(
    silver_df.write
    .mode("overwrite")
    .parquet(args.silver_output)
)
print(f"✅ Silver written to: {args.silver_output}")

spark.stop()
