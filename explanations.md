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
