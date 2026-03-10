"""
enrich_customers.py
───────────────────
Joins silver/customers with silver/user_profiles on email,
enriches missing fields, and writes the result to BigQuery gold layer.

Join key: email

Enrichment logic (coalesce — prefer existing customer data, fill from user_profiles):
  - state       → fill from user_profiles if null in customers
  - first_name  → fill from user_profiles if null in customers
  - last_name   → fill from user_profiles if null in customers

New fields added from user_profiles:
  - birth_date
  - phone_number
  - age (derived: years since birth_date)

Output: BigQuery table gold.user_profiles_enriched
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── 1. Argument parsing ─────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--customers_input",     required=False,
                    default="gs://robot-dream-course-data-lake/silver/customers/")
parser.add_argument("--user_profiles_input", required=False,
                    default="gs://robot-dream-course-data-lake/silver/user_profiles/")
parser.add_argument("--bq_project",          required=False,
                    default="robot-dream-course")
parser.add_argument("--bq_dataset",          required=False,
                    default="gold")
parser.add_argument("--bq_table",            required=False,
                    default="user_profiles_enriched")
parser.add_argument("--bq_temp_bucket",      required=False,
                    default="robot-dream-course-data-lake")
args = parser.parse_args()

# ── 2. Spark session ────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("enrich_customers")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("temporaryGcsBucket", args.bq_temp_bucket)

# ── 3. Read Silver layers ────────────────────────────────────────────────────
customers_df     = spark.read.parquet(args.customers_input)
user_profiles_df = spark.read.parquet(args.user_profiles_input)

customers_count     = customers_df.count()
user_profiles_count = user_profiles_df.count()
print(f"📥 Silver customers read:      {customers_count}")
print(f"📥 Silver user_profiles read:  {user_profiles_count}")

# ── 4. Join on email ─────────────────────────────────────────────────────────
#       Left join — keep all customers, enrich where user_profile exists.
#       Alias both sides to avoid column name conflicts after join.
joined_df = (
    customers_df.alias("c")
    .join(user_profiles_df.alias("up"), on="email", how="left")
)

# ── 5. Enrich — fill nulls from user_profiles, add new fields ───────────────
#       Use a single .select() so all alias references (c.*, up.*) resolve in
#       one pass.  Chaining .withColumn() would break alias resolution because
#       each call mutates the DataFrame schema and invalidates prior aliases.
enriched_df = joined_df.select(
    F.col("c.client_id").cast("long"),
    F.col("email"),
    F.coalesce(F.col("c.first_name"), F.col("up.first_name")).alias("first_name"),
    F.coalesce(F.col("c.last_name"),  F.col("up.last_name")).alias("last_name"),
    F.col("c.registration_date"),
    F.coalesce(F.col("c.state"), F.col("up.state")).alias("state"),
    F.col("up.birth_date").alias("birth_date"),
    F.floor(F.datediff(F.current_date(), F.col("up.birth_date")) / 365.25).alias("age"),
    F.col("up.phone_number").alias("phone_number"),
    F.current_timestamp().alias("processed_at"),
)

enriched_count = enriched_df.count()
print(f"📦 Enriched rows to write: {enriched_count}")

# ── 6. Write to BigQuery (gold layer) ────────────────────────────────────────
bq_table = f"{args.bq_project}.{args.bq_dataset}.{args.bq_table}"

(
    enriched_df.write
    .format("bigquery")
    .option("table", bq_table)
    .option("writeMethod", "direct")
    .mode("overwrite")
    .save()
)
print(f"✅ Gold written to BigQuery: {bq_table}")

spark.stop()
