"""
process_customers_raw_to_bronze.py
──────────────────────────────────
Reads raw customers CSVs from GCS and writes them to the Bronze layer as Parquet.
Bronze = faithful copy of raw data, with an added ingestion timestamp.

Delivery pattern: supplier sends a full table dump every day — each day's
folder contains all previous records plus new ones. Bronze stores everything
as-is; deduplication happens in the Bronze → Silver step.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# ── 1. Argument parsing ─────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--raw_input",     required=False,
                    default="gs://robot-dream-course-data-lake/raw/customers/")
parser.add_argument("--bronze_output", required=False,
                    default="gs://robot-dream-course-data-lake/bronze/customers/")
args = parser.parse_args()

# ── 2. Spark session ────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("process_customers_raw_to_bronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── 3. Explicit schema ───────────────────────────────────────────────────────
#       All fields kept as StringType — faithful to source, no casting here.
schema = StructType([
    StructField("Id",               StringType(), True),
    StructField("FirstName",        StringType(), True),
    StructField("LastName",         StringType(), True),
    StructField("Email",            StringType(), True),
    StructField("RegistrationDate", StringType(), True),
    StructField("State",            StringType(), True),
])

# ── 4. Read all raw CSVs (all daily dump folders) ───────────────────────────
raw_df = (
    spark.read
    .schema(schema)
    .option("header", True)
    .option("multiLine", False)
    .option("recursiveFileLookup", "true")
    .csv(args.raw_input)
)

raw_count = raw_df.count()

# ── 5. Minimal Bronze-layer enrichment ──────────────────────────────────────
#       No business logic — just track when the data was ingested.
bronze_df = raw_df.withColumn("ingested_at", F.current_timestamp())

# ── 6. Drop fully-null rows (completely empty lines in CSV) ─────────────────
bronze_df = bronze_df.dropna(how="all")

bronze_count = bronze_df.count()
print(f"📥 Raw rows read:       {raw_count}")
print(f"📦 Bronze rows written: {bronze_count}  (dropped {raw_count - bronze_count} empty rows)")

# ── 7. Write Bronze Parquet ─────────────────────────────────────────────────
#       Overwrite is correct — we always reload the full latest snapshot.
(
    bronze_df.write
    .mode("overwrite")
    .parquet(args.bronze_output)
)
print(f"✅ Bronze written to: {args.bronze_output}")

spark.stop()
