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
