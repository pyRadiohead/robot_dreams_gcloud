"""
process_sales_bronze_to_silver.py
─────────────────────────────────
Reads Bronze sales Parquet, cleanses and enriches it, writes to Silver.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, DateType

# ── 1. Argument parsing (keeps paths out of the script and matches the DAG) ─
parser = argparse.ArgumentParser()
parser.add_argument("--bronze_input",  required=False,
                    default="gs://robot-dream-course-data-lake/bronze/sales/")
parser.add_argument("--silver_output", required=False,
                    default="gs://robot-dream-course-data-lake/silver/sales/")
args = parser.parse_args()

# ── 2. Spark session ────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("process_sales_bronze_to_silver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── 3. Read Bronze Parquet ──────────────────────────────────────────────────
bronze_df = spark.read.parquet(args.bronze_input)

bronze_count = bronze_df.count()
print(f"📥 Bronze rows read: {bronze_count}")

# ── 4. Clean and normalize Price column ─────────────────────────────────────
# Remove any non-digit characters (like '$') and convert to Decimal(10,2)
# If the result is NULL (e.g. empty string), drop the row.
cleaned_df = (
    bronze_df
    .withColumn("price_clean", F.regexp_replace(F.col("Price"), "[^0-9.]", ""))
    .withColumn("price_clean", F.col("price_clean").cast(DecimalType(10, 2)))
    .dropna(subset=["price_clean"])
)

# ── 5. Cast typed columns and drop rows with nulls in key fields ────────────
cleaned_df = (
    cleaned_df
    .withColumn("CustomerId", F.col("CustomerId").cast(IntegerType()))
    .withColumn("PurchaseDate", F.to_date(F.col("PurchaseDate"), "yyyy-M-d"))
    .dropna(subset=["CustomerId", "PurchaseDate"])
)

# ── 6. Final select — rename columns, drop originals, add metadata ──────────
silver_df = cleaned_df.select(
    F.col("CustomerId").alias("client_id"),
    F.col("PurchaseDate").alias("purchase_date"),
    F.col("Product").alias("product_name"),
    F.col("price_clean").alias("price"),
    F.current_timestamp().alias("processed_at"),
)

silver_count = silver_df.count()
print(f"📦 Silver rows written: {silver_count}  (cleaned {bronze_count - silver_count} rows with bad price)")

# ── 7. Write Silver Parquet ─────────────────────────────────────────────────
(silver_df.write
 .mode("overwrite")
 .parquet(args.silver_output))

print(f"✅ Silver written to: {args.silver_output}")

spark.stop()