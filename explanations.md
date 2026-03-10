# Pipeline Fixes & Design Decisions

---

## 🔥 Spark Jobs

### `process_sales_raw_to_bronze.py`

| # | Type | Description |
|---|------|-------------|
| 1 | 🐛 Bug | `raw_count` used before assignment |

**`raw_count` used before assignment**
`raw_count` was referenced in the print statement but never assigned, causing a `NameError` at runtime. Fixed by adding `raw_count = raw_df.count()` before `dropna`.

---

### `process_sales_bronze_to_silver.py`

| # | Type | Description |
|---|------|-------------|
| 1 | 🐛 Bug | `price` cast to `IntegerType` silently truncated decimals |
| 2 | 🗑️ Redundant | No-op `when/otherwise` on price null check |
| 3 | ⚠️ Missing | `PurchaseDate` never parsed to a date type |
| 4 | ⚠️ Missing | Silent nulls after `CustomerId` cast |
| 5 | 📛 Naming | `updated_at` is misleading — renamed to `processed_at` |
| 6 | 🐛 Bug | Wrong date format `yyyy-MM-dd` didn't match raw data like `2022-09-1` |

**`price` cast to `IntegerType` silently truncated decimals**
After parsing `Price` into `Decimal(10,2)`, the value was immediately cast to `IntegerType`, dropping cents (e.g. `19.99` → `19`). Fixed by keeping `price` as `DecimalType(10, 2)`.

**No-op `when/otherwise` on price null check**
```python
.withColumn("price_clean", F.when(F.col("price_clean").isNull(), F.lit(None))
                            .otherwise(F.col("price_clean")))
```
Replaces `NULL` with `NULL` — has no effect. Removed.

**`PurchaseDate` never parsed to a date type**
`PurchaseDate` remained a raw string in Silver, making date-based filtering and aggregations unreliable downstream. Fixed by casting to `DateType` via `F.to_date(...)` and dropping rows where parse fails.

**Silent nulls after `CustomerId` cast**
Casting `CustomerId` to `IntegerType` silently converts non-numeric values to `NULL`. Fixed by adding `dropna(subset=["CustomerId", "PurchaseDate"])` after the casts.

**Wrong date format for raw data**
Format `yyyy-MM-dd` requires zero-padded days (`2022-09-01`), but raw CSVs contain single-digit days (`2022-09-1`). Every row would parse to `NULL` and be dropped silently. Found by inspecting a sample CSV:
```bash
# Confirmed format by reading raw data directly
gsutil ls gs://robot-dream-course-data-lake/raw/data/sales/
```
Fixed by changing to `yyyy-M-d` which handles both padded and unpadded days.

---

## ✈️ Airflow DAG (`process_sales.py`)

| # | Type | Description |
|---|------|-------------|
| 1 | 🐛 Bug | `submit_job` undefined — DAG would not parse |
| 2 | 🐛 Bug | Both tasks shared the same `batch_id` |
| 3 | 🐛 Bug | Cleanup only deleted one of two batches |
| 4 | 🔐 Auth | `google_cloud_default` connection not defined |
| 5 | 🔐 Auth | No retries — fail fast for easier debugging |
| 6 | 🔐 Auth | Execution service account not explicitly set |

**`submit_job` undefined**
The task chain referenced `submit_job` (never defined), causing an immediate `NameError` on DAG parse. Fixed:
```python
submit_raw_to_bronze >> submit_bronze_to_silver >> [cleanup_raw_to_bronze, cleanup_bronze_to_silver]
```

**Both tasks shared the same `batch_id`**
Dataproc Serverless requires unique batch IDs — the second submission always failed with a conflict. Fixed with distinct IDs: `BATCH_ID_RAW_TO_BRONZE` and `BATCH_ID_BRONZE_TO_SILVER`.

**Cleanup only deleted one of two batches**
The single `cleanup` task only deleted the bronze-to-silver batch, leaving raw-to-bronze accumulating stale records and causing ID conflicts on future reruns. Fixed with two parallel cleanup tasks.

**`google_cloud_default` connection not defined**
Airflow operators look up GCP connections by name in the metadata DB, not from `GOOGLE_APPLICATION_CREDENTIALS` directly. Fixed by adding to `docker-compose.yml`:
```yaml
AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: "google-cloud-platform://?key_path=%2Fopt%2Fairflow%2Fgcp%2Fsa-key.json&project=${GCP_PROJECT_ID}"
```
Airflow auto-creates connections from env vars prefixed with `AIRFLOW_CONN_`.

**Execution service account not explicitly set**
Without an explicit `service_account` in `execution_config`, Dataproc defaults to the Compute Engine default SA. Our SA didn't have permission to impersonate it. Fixed by setting:
```python
"execution_config": { "service_account": DATAPROC_SA }
```

---

## 🔐 IAM / GCP Permissions

| # | Error | Root Cause | Fix |
|---|-------|------------|-----|
| 1 | `403 dataproc.batches.create denied` | SA had `roles/dataproc.worker` but not editor | Added `roles/dataproc.editor` |
| 2 | `400 User not authorized to act as service account` | SA couldn't impersonate itself as execution SA | Granted `roles/iam.serviceAccountUser` on the SA to itself |

### Diagnosing permission issues

```bash
# Check what roles a service account currently has
gcloud projects get-iam-policy robot-dream-course \
  --flatten="bindings[].members" \
  --format="table(bindings.role,bindings.members)" \
  --filter="bindings.members:dataproc-spark-sa"
```

```bash
# Grant self-impersonation (required for Dataproc Serverless explicit SA)
gcloud iam service-accounts add-iam-policy-binding \
  dataproc-spark-sa@robot-dream-course.iam.gserviceaccount.com \
  --member="serviceAccount:dataproc-spark-sa@robot-dream-course.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"
```

---

## 📦 GCS Path Mismatches

Raw data and scripts were uploaded to wrong paths — the DAG expected flat paths, GCS had nested ones:

| Resource | Expected | Actual (in GCS) |
|----------|----------|-----------------|
| Raw sales CSVs | `raw/sales/` | `raw/data/sales/` |
| Spark job scripts | `jobs/process_sales_*.py` | `jobs/spark_jobs/process_sales_*.py` |

### Diagnosing path issues

```bash
# Inspect bucket structure
gsutil ls gs://robot-dream-course-data-lake/
gsutil ls gs://robot-dream-course-scripts/jobs/
```

### Fix — copy to correct paths

```bash
# Raw data
gsutil -m cp -r gs://robot-dream-course-data-lake/raw/data/sales/ \
               gs://robot-dream-course-data-lake/raw/sales

# Spark jobs (always upload from local to keep in sync with code changes)
gsutil cp spark_jobs/process_sales_raw_to_bronze.py    gs://robot-dream-course-scripts/jobs/
gsutil cp spark_jobs/process_sales_bronze_to_silver.py gs://robot-dream-course-scripts/jobs/
```

---

## 📊 CPU Quota Limitation

Dataproc Serverless requires **12 vCPUs minimum** per batch job. The project's global quota was exactly 12.

### Why exactly 12 vCPUs?

Dataproc Serverless runs Spark in a managed containerized environment. Each batch job always starts with:

| Component | Count | vCPUs each | Total vCPUs |
|-----------|-------|-----------|-------------|
| 🚗 Driver | 1 | 4 | 4 |
| ⚙️ Executor | 2 | 4 | 8 |
| **Total** | | | **12** |

**Driver (4 vCPUs)**
The Spark driver is the brain of the job. It runs the `main` Python script, builds the execution plan (DAG of stages/tasks), tracks task progress, and coordinates with executors. It does not process data itself — it only orchestrates. Dataproc Serverless allocates 4 vCPUs to the driver by default to ensure it can handle scheduling overhead without becoming a bottleneck.

**Executors (2 × 4 vCPUs)**
Executors are the workers that actually read, transform, and write data. Each executor runs Spark tasks in parallel across its cores. Dataproc Serverless enforces a **minimum of 2 executors** because:
- With only 1 executor, Spark cannot perform distributed shuffles (required for operations like `groupBy`, `join`, `sort`) — there would be no second partition to exchange data with
- 2 is the smallest number where Spark's parallelism model is meaningful

Each executor gets 4 vCPUs by default (matching the driver size), giving 4 parallel task slots per executor.

**Why not configure fewer cores?**
You can override executor/driver size via `runtime_config` in the batch spec, but Dataproc Serverless 2.x has a hard floor of 4 vCPUs per container (driver or executor). Smaller sizes are not supported — it's a managed service tradeoff for predictable performance.

**Practical implication for this project**
With `CPUS_ALL_REGIONS = 12`, only one batch can run at a time. The two jobs (raw→bronze, bronze→silver) are sequential in the DAG, which works — but the second job must wait for the first to fully release its quota before it can start.

### Diagnosing quota usage

```bash
# Check global CPU quota (CPUS_ALL_REGIONS)
gcloud compute project-info describe --project=robot-dream-course \
  --format="table(quotas.metric,quotas.limit,quotas.usage)"

# Check what instances are consuming quota
gcloud compute instances list --project=robot-dream-course \
  --format="table(name,zone,machineType,status)"

# Check if any Dataproc batches are stuck
gcloud dataproc batches list --project=robot-dream-course \
  --region=us-central1 \
  --format="table(name,state,createTime)"
```

**Result:** No VMs running, but quota showed 12/12 — the first batch had used all quota and the second was in `PENDING` state waiting for release.

**Fix:** Request a quota increase for `CPUS_ALL_REGIONS` to at least 24 via `IAM & Admin → Quotas` in GCP Console.

---

## 🐍 PySpark Data Engineering Reference

### 📥 Reading Data

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Always define schema explicitly — avoids inferSchema full scan
schema = StructType([
    StructField("id",    IntegerType(), True),
    StructField("name",  StringType(),  True),
    StructField("price", StringType(),  True),  # keep dirty fields as String
])

# CSV
df = spark.read.schema(schema).option("header", True).csv("gs://bucket/path/")

# Parquet (schema embedded — no need to specify)
df = spark.read.parquet("gs://bucket/path/")

# JSON Lines
df = spark.read.json("gs://bucket/path/")

# With multiple options
df = (spark.read
    .schema(schema)
    .option("header", True)
    .option("multiLine", False)      # False = one record per line (faster)
    .option("nullValue", "NULL")     # treat "NULL" string as null
    .option("emptyValue", "")        # treat empty string as empty, not null
    .csv("gs://bucket/path/"))
```

---

### 💾 Writing Data

```python
# Overwrite (full reload / daily batch)
df.write.mode("overwrite").parquet("gs://bucket/output/")

# Append (event streaming, incremental loads)
df.write.mode("append").parquet("gs://bucket/output/")

# Partitioned write (improves downstream query performance)
df.write.mode("overwrite").partitionBy("year", "month").parquet("gs://bucket/output/")

# Write as CSV (avoid in production — use Parquet)
df.write.mode("overwrite").option("header", True).csv("gs://bucket/output/")
```

---

### 🔧 Casting Types

```python
from pyspark.sql.types import IntegerType, LongType, DoubleType, DecimalType, DateType, TimestampType
from pyspark.sql import functions as F

# Basic cast
df = df.withColumn("id",    F.col("id").cast(IntegerType()))
df = df.withColumn("price", F.col("price").cast(DecimalType(10, 2)))  # precision=10, scale=2
df = df.withColumn("score", F.col("score").cast(DoubleType()))

# Cast date strings to DateType
df = df.withColumn("event_date", F.to_date(F.col("event_date"), "yyyy-MM-dd"))
df = df.withColumn("event_date", F.to_date(F.col("event_date"), "yyyy-M-d"))   # handles 2022-9-1

# Cast to timestamp
df = df.withColumn("created_at", F.to_timestamp(F.col("created_at"), "yyyy-MM-dd HH:mm:ss"))

# ⚠️ Cast silently returns NULL on failure — always check with dropna() or filter after
df = df.withColumn("id", F.col("id").cast(IntegerType()))
df = df.filter(F.col("id").isNotNull())  # drop rows where cast failed
```

---

### 🧹 Cleaning Strings

```python
# Remove non-numeric characters (e.g. "609$" → "609")
df = df.withColumn("price", F.regexp_replace(F.col("price"), "[^0-9.]", ""))

# Trim whitespace
df = df.withColumn("name", F.trim(F.col("name")))
df = df.withColumn("name", F.ltrim(F.col("name")))  # left trim
df = df.withColumn("name", F.rtrim(F.col("name")))  # right trim

# Upper / lower / title case
df = df.withColumn("state", F.upper(F.col("state")))
df = df.withColumn("name",  F.lower(F.col("name")))
df = df.withColumn("name",  F.initcap(F.col("name")))  # Title Case

# Replace values
df = df.withColumn("state", F.regexp_replace(F.col("state"), "N/A", ""))

# Split string into array
df = df.withColumn("tags", F.split(F.col("tags"), ","))

# Substring
df = df.withColumn("year", F.col("date_str").substr(1, 4))
```

---

### 🚫 Handling Nulls

```python
# Drop rows where ALL columns are null (empty CSV lines)
df = df.dropna(how="all")

# Drop rows where ANY column is null
df = df.dropna(how="any")

# Drop rows where specific columns are null
df = df.dropna(subset=["id", "email"])

# Fill nulls with a default value
df = df.fillna({"state": "Unknown", "score": 0})

# Fill nulls with another column's value
df = df.withColumn("display_name",
    F.coalesce(F.col("first_name"), F.col("username"), F.lit("Anonymous")))
# coalesce() returns the first non-null value from left to right

# Conditional replacement
df = df.withColumn("state",
    F.when(F.col("state").isNull(), F.lit("Unknown"))
     .otherwise(F.col("state")))

# Filter out nulls explicitly
df = df.filter(F.col("id").isNotNull())
df = df.filter(F.col("email").isNotNull() & F.col("state").isNotNull())
```

---

### 🔁 Deduplication

```python
from pyspark.sql.window import Window

# Simple dedup — drop exact duplicate rows
df = df.dropDuplicates()

# Dedup by key columns only
df = df.dropDuplicates(["id", "email"])

# ✅ Keep latest record per key (full snapshot / SCD pattern)
#    Used when source re-delivers all previous rows every day
window = Window.partitionBy("id").orderBy(F.col("updated_at").desc())

df = (df
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn"))

# Keep earliest record per key
window = Window.partitionBy("id").orderBy(F.col("created_at").asc())

# Keep record with highest value (e.g. most recent version number)
window = Window.partitionBy("id").orderBy(F.col("version").desc())
```

---

### 🏗️ Adding & Renaming Columns

```python
# Add a new column
df = df.withColumn("processed_at", F.current_timestamp())
df = df.withColumn("year", F.year(F.col("event_date")))
df = df.withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))

# Rename
df = df.withColumnRenamed("OldName", "new_name")

# Rename many columns at once (using select + alias)
df = df.select(
    F.col("Id").alias("client_id"),
    F.col("FirstName").alias("first_name"),
    F.col("PurchaseDate").alias("purchase_date"),
)

# Drop columns
df = df.drop("temp_column", "ingested_at")
```

---

### 🔗 Joins

```python
# Inner join (only matching rows)
result = customers.join(sales, customers["client_id"] == sales["customer_id"], "inner")

# Left join (all customers, matched sales or null)
result = customers.join(sales, "client_id", "left")

# Left join with different column names
result = customers.join(sales,
    customers["client_id"] == sales["customer_id"], "left")

# Avoid column name conflicts after join
result = customers.alias("c").join(sales.alias("s"),
    F.col("c.client_id") == F.col("s.customer_id"), "left") \
    .select("c.*", F.col("s.purchase_date"), F.col("s.price"))

# ⚠️ Broadcast join — use when one side is small (avoids shuffle)
from pyspark.sql.functions import broadcast
result = sales.join(broadcast(lookup_table), "product_id", "left")
```

---

### 📊 Aggregations

```python
# Basic aggregations
df.groupBy("state").count().show()

df.groupBy("state", "product").agg(
    F.count("*").alias("total_orders"),
    F.sum("price").alias("total_revenue"),
    F.avg("price").alias("avg_price"),
    F.min("price").alias("min_price"),
    F.max("price").alias("max_price"),
    F.countDistinct("client_id").alias("unique_customers"),
)

# Window aggregations (without collapsing rows)
window = Window.partitionBy("state").orderBy("purchase_date")

df = df.withColumn("running_total", F.sum("price").over(window))
df = df.withColumn("rank_in_state", F.rank().over(window))
df = df.withColumn("prev_purchase", F.lag("price", 1).over(window))
df = df.withColumn("next_purchase", F.lead("price", 1).over(window))
```

---

### 🗓️ Date & Time Operations

```python
# Extract parts
df = df.withColumn("year",    F.year(F.col("purchase_date")))
df = df.withColumn("month",   F.month(F.col("purchase_date")))
df = df.withColumn("day",     F.dayofmonth(F.col("purchase_date")))
df = df.withColumn("weekday", F.dayofweek(F.col("purchase_date")))  # 1=Sunday

# Date arithmetic
df = df.withColumn("due_date",   F.date_add(F.col("purchase_date"), 30))
df = df.withColumn("days_since", F.datediff(F.current_date(), F.col("purchase_date")))

# Truncate to period (useful for grouping)
df = df.withColumn("week",  F.date_trunc("week",  F.col("purchase_date")))
df = df.withColumn("month", F.date_trunc("month", F.col("purchase_date")))

# Current time
df = df.withColumn("processed_at", F.current_timestamp())
df = df.withColumn("today",        F.current_date())

# Format date as string
df = df.withColumn("date_str", F.date_format(F.col("purchase_date"), "yyyy-MM-dd"))
```

---

### 🔍 Filtering

```python
# Basic filters
df = df.filter(F.col("price") > 100)
df = df.filter(F.col("state") == "Texas")
df = df.filter(F.col("state") != "Unknown")
df = df.filter(F.col("state").isNotNull())
df = df.filter(F.col("state").isNull())

# Multiple conditions
df = df.filter((F.col("price") > 100) & (F.col("state") == "Texas"))
df = df.filter((F.col("state") == "Texas") | (F.col("state") == "Ohio"))

# IN list
df = df.filter(F.col("state").isin("Texas", "Ohio", "Maine"))
df = df.filter(~F.col("state").isin("Unknown", "N/A"))  # NOT IN

# String matching
df = df.filter(F.col("email").contains("@gmail.com"))
df = df.filter(F.col("name").startswith("A"))
df = df.filter(F.col("name").endswith("son"))
df = df.filter(F.col("email").rlike(r"^[a-zA-Z0-9_.]+@[a-zA-Z0-9.]+\.[a-zA-Z]{2,}$"))

# Between (inclusive)
df = df.filter(F.col("age").between(20, 30))
```

---

### 🧮 SQL on DataFrames

```python
# Register DataFrame as a temp view and query with SQL
df.createOrReplaceTempView("sales")

result = spark.sql("""
    SELECT
        state,
        COUNT(*)          AS total_orders,
        SUM(price)        AS total_revenue,
        AVG(price)        AS avg_price
    FROM sales
    WHERE price > 0
      AND state IS NOT NULL
    GROUP BY state
    ORDER BY total_revenue DESC
""")

# MERGE equivalent in PySpark (upsert — no native MERGE, use Delta Lake or manual logic)
# Manual upsert: union new + old, deduplicate keeping new
updates = new_data.withColumn("_src", F.lit("new"))
existing = old_data.withColumn("_src", F.lit("old"))

merged = (updates.unionByName(existing)
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("id").orderBy(F.col("_src").asc())))  # "new" sorts first
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_src"))
```

---

### ⚡ Performance Tips

| Technique | When to use | How |
|-----------|-------------|-----|
| **Cache** | DataFrame used multiple times in same job | `df.cache()` or `df.persist()` |
| **Broadcast join** | One table is small (< ~10 MB) | `sales.join(broadcast(lookup), "id")` |
| **Avoid `count()` loops** | Multiple counts cause multiple scans | Compute once, store in variable |
| **Explicit schema** | Every CSV read | Skips inferSchema full scan |
| **Partition pruning** | Querying partitioned data | Filter on partition column early |
| **`select` early** | Large DataFrames | Drop unused columns before joins/groupBy |
| **Avoid UDFs** | Performance-critical transforms | Use native `F.*` functions — they run in JVM |

```python
# ✅ Cache when reusing a DataFrame
bronze_df = spark.read.parquet("gs://...").cache()
count_before = bronze_df.count()   # triggers cache population
count_after  = bronze_df.filter(...).count()  # reads from cache

# ✅ Select only needed columns early
df = df.select("id", "price", "state", "purchase_date")  # drop the rest

# ❌ Avoid Python UDFs — they serialize data out of JVM
# from pyspark.sql.functions import udf  ← slow
# ✅ Use built-in functions instead
df = df.withColumn("price_clean", F.regexp_replace(F.col("price"), "[^0-9.]", ""))
```

---

## 🏅 Final Analytical Task

> **In which state were the most TVs purchased by customers aged 20–30 in the first decade of September?**

The gold layer (`gold.user_profiles_enriched`) holds enriched customer data. Silver sales (`silver/sales/`) is stored in GCS as Parquet. To query both together in BigQuery, first expose silver/sales as a BigQuery external table, then join and aggregate.

### Step 1 — Create external table for silver/sales

```sql
CREATE OR REPLACE EXTERNAL TABLE `robot-dream-course.silver.sales`
OPTIONS (
  format = 'PARQUET',
  uris   = ['gs://robot-dream-course-data-lake/silver/sales/*.parquet',
             'gs://robot-dream-course-data-lake/silver/sales/**/*.parquet']
);
```

> 💡 This requires the `silver` dataset to exist. Create it if needed:
> ```bash
> bq mk --dataset --location=us-central1 robot-dream-course:silver
> ```

### Step 2 — Run the analytical query

```sql
SELECT
    e.state,
    COUNT(*)  AS tv_purchases
FROM `robot-dream-course.silver.sales`  AS s
JOIN `robot-dream-course.gold.user_profiles_enriched` AS e
    ON s.client_id = e.client_id
WHERE
    s.product_name = 'TV'
    AND e.age BETWEEN 20 AND 30
    AND EXTRACT(MONTH FROM s.purchase_date) = 9
    AND EXTRACT(DAY   FROM s.purchase_date) BETWEEN 1 AND 10
GROUP BY e.state
ORDER BY tv_purchases DESC
LIMIT 1;
```

### Via `bq` CLI

```bash
bq query --use_legacy_sql=false \
  --project_id=robot-dream-course \
'SELECT
    e.state,
    COUNT(*) AS tv_purchases
FROM `robot-dream-course.silver.sales` AS s
JOIN `robot-dream-course.gold.user_profiles_enriched` AS e
    ON s.client_id = e.client_id
WHERE
    s.product_name = "TV"
    AND e.age BETWEEN 20 AND 30
    AND EXTRACT(MONTH FROM s.purchase_date) = 9
    AND EXTRACT(DAY   FROM s.purchase_date) BETWEEN 1 AND 10
GROUP BY e.state
ORDER BY tv_purchases DESC
LIMIT 1'
```

**Prerequisites before running:**
1. `process_sales` DAG completed (silver/sales populated in GCS)
2. `process_customers` + `process_user_profiles` DAGs completed
3. `enrich_customers` DAG completed (`gold.user_profiles_enriched` in BigQuery)
4. External table `silver.sales` created (Step 1 above)

---

## 🗄️ BigQuery via gcloud / bq CLI

BigQuery has its own CLI tool `bq` that ships with the Google Cloud SDK alongside `gcloud`. Both are used for BigQuery operations.

### 🔧 Setup

```bash
# Authenticate
gcloud auth login
gcloud config set project robot-dream-course

# bq uses the same auth as gcloud — no separate login needed
bq show --project_id=robot-dream-course
```

---

### 📁 Datasets

```bash
# List all datasets in project
bq ls --project_id=robot-dream-course

# Create a dataset
bq mk --dataset \
  --location=us-central1 \
  --description="Gold layer" \
  robot-dream-course:gold

# Delete a dataset (and all tables inside)
bq rm -r -f robot-dream-course:gold

# Show dataset details (location, labels, etc.)
bq show robot-dream-course:gold
```

---

### 📋 Tables

```bash
# List tables in a dataset
bq ls robot-dream-course:gold

# Show table schema
bq show robot-dream-course:gold.user_profiles_enriched

# Show table schema in JSON (useful for scripting)
bq show --format=prettyjson robot-dream-course:gold.user_profiles_enriched \
  | jq '.schema.fields'

# Preview first 10 rows
bq head -n 10 robot-dream-course:gold.user_profiles_enriched

# Row count
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `robot-dream-course.gold.user_profiles_enriched`'

# Delete a table
bq rm -f robot-dream-course:gold.user_profiles_enriched
```

---

### 🔍 Running Queries

```bash
# Inline query (standard SQL)
bq query --use_legacy_sql=false \
  'SELECT state, COUNT(*) AS cnt
   FROM `robot-dream-course.gold.user_profiles_enriched`
   GROUP BY state
   ORDER BY cnt DESC
   LIMIT 10'

# Query from a .sql file
bq query --use_legacy_sql=false < my_query.sql

# Save results to a table
bq query --use_legacy_sql=false \
  --destination_table=robot-dream-course:gold.query_results \
  --replace \
  'SELECT * FROM `robot-dream-course.gold.user_profiles_enriched` WHERE age < 25'

# Dry run — estimate bytes scanned before running (cost check)
bq query --use_legacy_sql=false --dry_run \
  'SELECT * FROM `robot-dream-course.gold.user_profiles_enriched`'
```

---

### 📤 Loading Data

```bash
# Load CSV from GCS into a table
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --autodetect \
  robot-dream-course:bronze.sales \
  'gs://robot-dream-course-data-lake/bronze/sales/*.csv'

# Load Parquet from GCS
bq load \
  --source_format=PARQUET \
  robot-dream-course:silver.customers \
  'gs://robot-dream-course-data-lake/silver/customers/*.parquet'
```

---

### 🔗 External Tables (query GCS Parquet directly)

External tables let BigQuery query files in GCS without loading them — data stays in GCS, BigQuery just reads it on demand.

```bash
# Create external table pointing to GCS Parquet
bq mkdef \
  --source_format=PARQUET \
  'gs://robot-dream-course-data-lake/silver/sales/*.parquet' \
  > /tmp/silver_sales_def.json

bq mk \
  --external_table_definition=/tmp/silver_sales_def.json \
  robot-dream-course:silver.sales
```

Or directly in SQL (simpler):

```sql
CREATE OR REPLACE EXTERNAL TABLE `robot-dream-course.silver.sales`
OPTIONS (
  format = 'PARQUET',
  uris   = ['gs://robot-dream-course-data-lake/silver/sales/**']
);
```

| Feature | Native Table | External Table |
|---------|-------------|----------------|
| Storage | BigQuery managed | GCS (your bucket) |
| Query speed | Fast | Slower (reads GCS each time) |
| Cost | Storage + query | Query only (no BQ storage cost) |
| Best for | Gold layer, frequent queries | Silver layer, occasional analytics |

---

### ⚙️ Jobs & Monitoring

```bash
# List recent BigQuery jobs
bq ls --jobs --all --max_results=10 --project_id=robot-dream-course

# Show specific job details (find id from ls above)
bq show --job --project_id=robot-dream-course <job_id>

# Cancel a running job
bq cancel --project_id=robot-dream-course <job_id>
```

---

### 💡 Best Practices

| Practice | Why |
|----------|-----|
| Always use `--use_legacy_sql=false` | Legacy SQL is outdated; standard SQL is the default in console but not in CLI |
| Use `--dry_run` before large queries | Estimates bytes scanned → cost before committing |
| Partition gold tables by date | Queries that filter by date scan only relevant partitions, reducing cost |
| Prefer external tables for silver | Avoids double-storing data already in GCS |
| Use `bq show` before deleting | Confirm you're deleting the right table/dataset |
| Prefer `bq query` over `gcloud` for SQL | `gcloud` doesn't have native BigQuery query support — `bq` is the right tool |

---

## 🔗 Data Enrichment — `enrich_customers.py`

Enrichment is the process of combining two or more data sources to fill missing fields and add new ones. In this pipeline, `silver/customers` has incomplete data (missing state, names) — `silver/user_profiles` provides the missing pieces. The join key is `email`.

### Why left join?

```python
joined_df = customers_df.alias("c").join(user_profiles_df.alias("up"), on="email", how="left")
```

A **left join** preserves all customer rows even if no matching user_profile exists. This matters because:
- Not every customer may have a user_profile (they registered before profiles were collected)
- Losing customers in gold would break downstream analytics (under-counting by state/age)
- Customers without a profile get `null` for `birth_date`, `age`, `phone_number` — which is correct and queryable

An **inner join** would silently drop every customer without a profile.

---

### Why alias both DataFrames?

After joining two DataFrames that share column names (both have `first_name`, `last_name`, `state`), Spark cannot resolve `F.col("first_name")` — it's ambiguous. Aliasing with `.alias("c")` and `.alias("up")` gives each side a namespace:

```python
F.col("c.first_name")   # customers side
F.col("up.first_name")  # user_profiles side
```

Without aliases, Spark raises `AnalysisException: Reference 'first_name' is ambiguous`.

---

### `coalesce()` for null-filling

```python
.withColumn("first_name", F.coalesce(F.col("c.first_name"), F.col("up.first_name")))
.withColumn("last_name",  F.coalesce(F.col("c.last_name"),  F.col("up.last_name")))
.withColumn("state",      F.coalesce(F.col("c.state"),      F.col("up.state")))
```

`coalesce(a, b)` returns the **first non-null value** left-to-right. The ordering is intentional:
- **Customer data is preferred** (`c.*` is listed first) — if a customer already has a state, we keep it
- **User profile fills the gap** — only used when the customer field is null

This is safer than unconditionally overwriting customer fields with profile data, which could replace verified customer data with inferred profile data.

---

### Derived `age` field

```python
.withColumn("age", F.floor(F.datediff(F.current_date(), F.col("up.birth_date")) / 365.25))
```

| Part | Explanation |
|------|-------------|
| `F.datediff(current_date, birth_date)` | Returns total days since birth as integer |
| `/ 365.25` | Converts days to years; `365.25` accounts for leap years (every 4 years adds 0.25 day/year average) |
| `F.floor(...)` | Rounds down to full years completed — `29.9` → `29`, matching how age works legally |

The field comes from `up.birth_date` (not `c.*`) because customers Silver has no birth date — it only exists in user_profiles.

---

### Field source summary

| Output column | Source | Logic |
|---------------|--------|-------|
| `client_id` | customers | Direct |
| `email` | customers (join key) | Direct |
| `first_name` | customers → user_profiles | `coalesce(c, up)` |
| `last_name` | customers → user_profiles | `coalesce(c, up)` |
| `registration_date` | customers | Direct |
| `state` | customers → user_profiles | `coalesce(c, up)` |
| `birth_date` | user_profiles | Direct (null if no profile) |
| `age` | user_profiles | Derived from birth_date |
| `phone_number` | user_profiles | Direct (null if no profile) |
| `processed_at` | runtime | `F.current_timestamp()` |

---

### Writing to BigQuery

```python
spark.conf.set("temporaryGcsBucket", args.bq_temp_bucket)

enriched_df.write.format("bigquery") \
    .option("table", bq_table) \
    .mode("overwrite") \
    .save()
```

The `spark-bigquery-connector` requires a `temporaryGcsBucket` because:
1. Spark serializes data to GCS as intermediate Avro/Parquet files
2. BigQuery then loads from those files internally
3. The temp files are automatically cleaned up after load

Without `temporaryGcsBucket`, the write will fail with a missing bucket error. The bucket must be in the same region as the BigQuery dataset (`us-central1` in this project).

---

## 🐛 Debugging the Full Pipeline — Issues Found and Fixed

This section documents every issue encountered when running the complete pipeline end-to-end (raw → bronze → silver → gold → BigQuery → analytical query), in the order they appeared.

---

### Issue 1: `recursiveFileLookup` — Spark reads 0 rows from nested GCS folders

**Symptom:** `process_sales_raw_to_bronze.py` wrote 0 rows to bronze, despite raw CSVs being present in GCS.

**Root cause:** Raw data lives in date-partitioned subdirectories:
```
gs://robot-dream-course-data-lake/raw/sales/
  ├── 2022-09-1/2022-09-1__sales.csv
  ├── 2022-09-2/2022-09-2__sales.csv
  └── ...
```

`spark.read.csv("gs://bucket/raw/sales/")` by default only reads files **directly** in the specified directory. The CSV files are one level deeper (inside date folders), so Spark found zero files.

**Fix:** Add `recursiveFileLookup` option to recurse into subdirectories:
```python
# Before (0 rows)
raw_df = spark.read.schema(schema).option("header", True).csv(args.raw_input)

# After (71,085 rows)
raw_df = (
    spark.read
    .schema(schema)
    .option("header", True)
    .option("recursiveFileLookup", "true")   # ← recurse into subdirs
    .csv(args.raw_input)
)
```

**Applied to:** `process_sales_raw_to_bronze.py`, `process_customers_raw_to_bronze.py`

> ⚠️ `recursiveFileLookup` disables partition discovery (Spark won't infer partition columns from folder names like `year=2022/`). This is fine for our use case — we don't need Spark to infer partitions from raw data paths.

---

### Issue 2: `COLUMN_ALREADY_EXISTS` — Parquet case-insensitive column conflict

**Symptom:** `process_sales_bronze_to_silver.py` failed with:
```
AnalysisException: [COLUMN_ALREADY_EXISTS] The column `price` already exists.
```

**Root cause:** The bronze Parquet has a column called `Price` (capital P, from the original CSV). The script created a `price_clean` column, then tried to rename it:
```python
.withColumnRenamed("price_clean", "price")   # creates lowercase "price"
```
Now the DataFrame has BOTH `Price` (original) and `price` (renamed). Parquet is case-insensitive for column names, so it sees two columns with the same name.

**Fix:** Replace the rename with a clean `.select()` that picks only the desired columns:
```python
# Before — leaves original Price in DataFrame
silver_df = cleaned_df.withColumn("processed_at", F.current_timestamp()) \
                      .withColumnRenamed("price_clean", "price")

# After — explicit column list, no name conflicts
silver_df = cleaned_df.select(
    F.col("CustomerId").alias("client_id"),
    F.col("PurchaseDate").alias("purchase_date"),
    F.col("Product").alias("product_name"),
    F.col("price_clean").alias("price"),          # clean rename
    F.current_timestamp().alias("processed_at"),
)
```

> 💡 **Best practice:** After joins or transformations that add/rename columns, prefer `.select()` with an explicit column list over `.withColumnRenamed()`. It makes the output schema obvious and prevents ghost columns from surviving.

---

### Issue 3: `UNRESOLVED_COLUMN` — Alias invalidation after chained `.withColumn()`

**Symptom:** `enrich_customers.py` failed with:
```
AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION]
A column or function parameter with name `up`.`birth_date` cannot be resolved.
Did you mean one of the following? [`birth_date`, `first_name`, `first_name`, `last_name`, `last_name`]
```

**Root cause:** After a join with aliases (`c` and `up`), chaining `.withColumn()` calls invalidates alias references:

```python
joined_df = customers.alias("c").join(profiles.alias("up"), on="email", how="left")

enriched = (
    joined_df
    .withColumn("first_name", F.coalesce(F.col("c.first_name"), F.col("up.first_name")))
    # ↑ This creates a NEW top-level "first_name" column.
    #   The DataFrame schema now has: c.first_name, up.first_name, AND first_name.
    #   Spark's internal alias resolution gets confused.

    .withColumn("birth_date", F.col("up.birth_date"))
    # ↑ FAILS: "up.birth_date" can no longer be resolved because
    #   the previous .withColumn() mutated the DataFrame schema
    #   and invalidated the "up" alias namespace.
)
```

The suggestion in the error (`Did you mean: birth_date, first_name, first_name`) shows the duplicate columns — two `first_name` columns exist (from `c` and `up`), plus the new one created by `withColumn`.

**Fix:** Resolve ALL alias references in a single `.select()`:
```python
# All c.* and up.* references are resolved in one pass — no alias invalidation
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
```

> 💡 **Rule of thumb:** After a join with aliases, do NOT chain `.withColumn()` calls that reference aliases. Use a single `.select()` instead. Each `.withColumn()` creates a new DataFrame, and Spark may lose track of which alias namespace a column belongs to.

---

### Issue 4: Parquet INT32 without `ConvertedType` — BigQuery rejects the load

**Symptom:** `enrich_customers.py` successfully computed 47,469 enriched rows but failed writing to BigQuery:
```
BigQueryException: Got non-null value in column client_id of parquet type INT32.
Parquet column requires appropriate ConvertedType to load.
```

After casting `client_id` to `LongType` (INT64), the same error persisted:
```
Got non-null value in column client_id of parquet type INT64.
Parquet column requires appropriate ConvertedType to load.
```

**Root cause:** The `spark-bigquery-connector` uses an **indirect write method** by default:

```
Spark DataFrame → write Parquet to GCS → BigQuery LOAD JOB reads Parquet → table created
```

Spark's Parquet writer creates files with bare `INT32`/`INT64` physical types **without** the Parquet `ConvertedType` annotation (like `INT_32` or `INT_64`). BigQuery's Parquet loader requires these annotations to correctly interpret the data types. Without them, the load job fails.

**Attempted fixes and results:**

| Attempt | Option | Result |
|---------|--------|--------|
| 1 | Cast `client_id` to `LongType` (INT64) | ❌ Same error — INT64 also needs `ConvertedType` |
| 2 | `.option("intermediateFormat", "avro")` | ❌ `spark-avro` module not included in Dataproc Serverless 2.1 runtime |
| 3 | `.option("writeMethod", "direct")` | ✅ Bypasses Parquet entirely |

**Fix:** Use the BigQuery Storage Write API (`direct` method) which streams data directly from Spark to BigQuery without intermediate files:
```python
enriched_df.write \
    .format("bigquery") \
    .option("table", bq_table) \
    .option("writeMethod", "direct")   # ← bypasses intermediate Parquet
    .mode("overwrite") \
    .save()
```

**How `writeMethod` works:**

| Method | Flow | Pros | Cons |
|--------|------|------|------|
| `indirect` (default) | Spark → Parquet/Avro on GCS → BQ Load Job | Simple, well-tested | Parquet `ConvertedType` issues, needs `temporaryGcsBucket`, slower |
| `direct` | Spark → BigQuery Storage Write API | No intermediate files, no type annotation issues, faster | Requires `bigquerystorage.tables.create` permission |

> ⚠️ The `indirect` method with Parquet intermediate format is a known compatibility issue between Spark's Parquet writer and BigQuery's Parquet loader. If you must use `indirect`, the only reliable intermediate format is Avro — but `spark-avro` must be available on the cluster (`--packages org.apache.spark:spark-avro_2.13:3.4.0`). Dataproc Serverless 2.1 does not include it by default.

---

### Issue 5: Incompatible schema — Old empty BigQuery table blocks overwrite

**Symptom:** Even after fixing the write method to `direct`, the job still failed:
```
BigQueryConnectorException$InvalidSchemaException:
Destination table's schema is not compatible with dataframe's schema
```

**Root cause:** A previous failed run of `enrich_customers.py` had created the `gold.user_profiles_enriched` table with a wrong schema:

| Column | Old (wrong) table | New (correct) DataFrame |
|--------|-------------------|------------------------|
| `client_id` | STRING | INTEGER (INT64) |
| `full_name` | present | absent |
| `age` | absent | present |

The `direct` write method with `mode("overwrite")` tries to overwrite the data but validates the schema first against the existing table. When the schemas are incompatible, it rejects the write.

**Fix:** Drop the stale table before writing:
```bash
bq rm -f robot-dream-course:gold.user_profiles_enriched
```

Then the Spark job creates the table fresh with the correct schema.

> 💡 This only happens when an old table exists with a different schema. Once the pipeline works, subsequent `mode("overwrite")` runs will succeed because the schema stays consistent.

---

### Issue 6: BigQuery dataset region mismatch — `silver` in `us-central1`, `gold` in `US`

**Symptom:** The analytical query joining `silver.sales` and `gold.user_profiles_enriched` failed:
```
Not found: Dataset robot-dream-course:silver was not found in location US
```

**Root cause:** BigQuery cannot run cross-region queries. The two datasets were in different regions:

| Dataset | Location | Created by |
|---------|----------|------------|
| `gold` | `US` | `spark-bigquery-connector` (defaults to `US` when not specified) |
| `silver` | `us-central1` | Manual `bq mk --location=us-central1` |

BigQuery determines the query's execution location from the tables referenced. With tables in different regions, the query can't locate one of them.

**Fix:** Recreate the `silver` dataset in `US` to match `gold`:
```bash
# Drop tables first (dataset can't be deleted while non-empty)
bq rm -f robot-dream-course:silver.sales
bq rm -f --dataset robot-dream-course:silver

# Recreate in US
bq mk --dataset --location=US robot-dream-course:silver

# Recreate external table
bq query --use_legacy_sql=false \
  'CREATE OR REPLACE EXTERNAL TABLE `robot-dream-course.silver.sales`
   OPTIONS (format = "PARQUET",
            uris = ["gs://robot-dream-course-data-lake/silver/sales/*.parquet"])'
```

> 💡 External tables in BigQuery can reference GCS data in any region — only the BigQuery **dataset metadata** must be co-located. So a `silver` dataset in `US` can point to Parquet files in a `us-central1` bucket.

> ⚠️ The `spark-bigquery-connector` creates datasets in `US` by default when they don't exist. To control this, either pre-create the dataset with the desired location or set the `createDisposition` option.

---

### Issue 7: Dataproc Serverless quota exhaustion — CPU and disk

**Symptom:** Batch submissions failed intermittently with:
```
Insufficient 'CPUS_ALL_REGIONS' quota. Requested 12.0, available 0.0.
Insufficient 'DISKS_TOTAL_GB' quota. Requested 1200.0, available 848.0.
```

**Root cause:** Each Dataproc Serverless batch requires **12 vCPUs** (1 driver × 4 + 2 executors × 4) and **1,200 GB disk** (400 GB × 3 nodes). The project quota was exactly 12 vCPUs. Failed batches don't release quota instantly — GCE needs time (~30–90 seconds) to deprovision the underlying VMs and free the quota.

Running multiple batches rapidly (especially after failures) caused quota pile-up.

**Fix:** Wait 60–90 seconds between batch submissions after failures. For long-term reliability:
```bash
# Check current quota usage
gcloud compute project-info describe --project=robot-dream-course \
  --format="json(quotas)" | python3 -c "
import sys, json
for q in json.load(sys.stdin)['quotas']:
    if 'CPU' in q['metric'] or 'DISK' in q['metric']:
        print(f\"{q['metric']}: {q['usage']}/{q['limit']}\")"

# Check for stuck/running batches
gcloud dataproc batches list --project=robot-dream-course \
  --region=us-central1 --filter="state=RUNNING OR state=PENDING" \
  --format="table(name.basename(),state)"
```

---

### Summary: Full Error Chain

| # | Error | Layer | Root cause | Fix |
|---|-------|-------|------------|-----|
| 1 | 0 rows in bronze | Spark read | No `recursiveFileLookup` for nested GCS dirs | `.option("recursiveFileLookup", "true")` |
| 2 | `COLUMN_ALREADY_EXISTS` | Spark write | `Price` + `price` both in DataFrame | `.select()` with explicit columns |
| 3 | `UNRESOLVED_COLUMN: up.birth_date` | Spark transform | `.withColumn()` chain breaks alias resolution | Single `.select()` for all alias refs |
| 4 | `Parquet INT32 without ConvertedType` | BQ load | Spark's Parquet lacks type annotations BQ needs | `.option("writeMethod", "direct")` |
| 5 | `InvalidSchemaException` | BQ write | Old table had wrong schema | `bq rm -f` the stale table |
| 6 | `Dataset not found in location US` | BQ query | `silver` in `us-central1`, `gold` in `US` | Recreate `silver` dataset in `US` |
| 7 | `Insufficient quota` | GCE infra | 12/12 vCPU quota, slow release after failures | Wait between runs, request quota increase |
