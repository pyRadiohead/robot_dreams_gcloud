"""
process_sales_raw_to_bronze.py
────────────────────────────
Reads raw sales CSVs from GCS and writes them to the Bronze layer as Parquet.
Bronze = faithful copy of raw data, with an added ingestion timestamp.

Usage (Dataproc Serverless / spark-submit):
    --raw_input     GCS path to raw sales CSVs   (e.g. gs://.../raw/sales/) 
    --bronze_output GCS path to write Parquet to (e.g. gs://.../bronze/sales/)
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# ── 1. Argument parsing (keeps paths out of the script and matches the DAG) ─
parser = argparse.ArgumentParser()
parser.add_argument("--raw_input",     required=False,
                    default="gs://robot-dream-course-data-lake/raw/sales/")
parser.add_argument("--bronze_output", required=False,
                    default="gs://robot-dream-course-data-lake/bronze/sales/")
args = parser.parse_args()

# ── 2. Spark session ────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("process_sales_raw_to_bronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── 3. Explicit schema — avoids slow inferSchema scan and handles dirty data ─
#       Price is kept as StringType here because raw data has mixed formats
#       (e.g. "609$" vs "4966"). Cleaning happens in the Bronze → Silver step.
schema = StructType([
    StructField("CustomerId",   StringType(), True),
    StructField("PurchaseDate", StringType(), True),
    StructField("Product",      StringType(), True),
    StructField("Price",        StringType(), True),  # dirty: "609$" vs "4966"
])

# ── 4. Read raw CSVs ────────────────────────────────────────────────────────
raw_df = (
    spark.read
    .schema(schema)
    .option("header", True)
    .option("multiLine", False)
    .csv(args.raw_input)
)


# ── 5. Minimal Bronze-layer enrichment ─────────────────────────────────────
#       No business logic — just track when the data was ingested.
bronze_df = raw_df.withColumn("ingested_at", F.current_timestamp())

# ── 6. Drop fully-null rows (completely empty lines in CSV) ─────────────────
raw_count = raw_df.count()
bronze_df = bronze_df.dropna(how="all")

bronze_count = bronze_df.count()
print(f"📦 Bronze rows written: {bronze_count}  (dropped {raw_count - bronze_count} empty rows)")

# ── 7. Write Bronze Parquet ─────────────────────────────────────────────────
(
    bronze_df.write
    .mode("overwrite")  # overwrite is ok because this is a daily batch
    .parquet(args.bronze_output)
)
print(f"✅ Bronze written to: {args.bronze_output}")

spark.stop()    