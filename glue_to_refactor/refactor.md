# Glue Jobs Refactor Analysis

Two separate solutions to the same pipeline problem, reviewed independently.

## File Attribution

| File | Author | Layer |
|------|--------|-------|
| `process_sales.py` | Vlad | Bronze → Silver |
| `process_customers.py` | Vlad | Bronze → Silver |
| `process_user_profiles.py` | Vlad | Bronze → Silver |
| `sales_bronze_to_silver.py` | Sergii | Bronze → Silver |
| `customers_bronze_to_silver.py` | Sergii | Bronze → Silver |
| `user_profiles_raw_to_silver.py` | Sergii | Raw → Silver |
| `gold_sales_enriched.py` | Sergii | Gold |

---

## Vlad's Solution (`process_*.py`)

### 🐛 Wrong Deduplication in `process_customers.py`
```python
customers_silver = customers_raw.select(...).distinct()
```
`.distinct()` deduplicates on the combination of **all selected columns**. For a full daily dump, if a customer's data ever changes (e.g. state updated), `.distinct()` keeps both versions because they differ on one column — resulting in multiple rows per `client_id` in Silver. Deduplication should be keyed on `client_id` only, keeping the latest record per ID.

---

### 🐛 `full_name` Never Split in `process_user_profiles.py`
```python
user_profiles_silver = user_profiles_raw.select(
    col("email"),
    col("full_name"),   # kept as-is, never decomposed
    ...
)
```
`full_name` passes through to Silver without being split into `first_name` / `last_name`. Downstream enrichment joins on `email` to fill customer name fields — but the profiles source never produces usable name columns. The enrichment purpose for names is defeated.

---

### ⚠️ Double `timeParserPolicy` Configuration (all three files)
```python
sc._jsc.hadoopConfiguration().set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# ...
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
```
Set twice — once on the Hadoop configuration and again on the Spark session. `spark.conf.set` alone is sufficient. More importantly, relying on `LEGACY` mode is a workaround masking an underlying date format issue. The better approach is to normalize date strings explicitly with `lpad` before calling `to_date` — no `LEGACY` mode needed, and the intent is clear.

---

### ⚠️ `price` Cast to `double` in `process_sales.py`
```python
col("clean_price").cast("double").alias("price")
```
`double` is a floating point type and is imprecise for monetary values — `0.1 + 0.2 != 0.3` in IEEE 754. The correct type for money is `DecimalType(10, 2)`, which provides exact fixed-point arithmetic.

---

### 🗑️ Unused Wildcard Import (all three files)
```python
from awsglue.transforms import *
```
Nothing from `awsglue.transforms` is used in any of these files. Wildcard imports pollute the namespace and make the dependencies opaque.

---

### 📛 Dead Parameter `SILVER_DATABASE` (all three files)
```python
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_DATABASE', 'SILVER_DATABASE', ...])
# SILVER_DATABASE never referenced anywhere in the code body
```
A parameter accepted but silently ignored is misleading — the caller has no way to know it does nothing.

---

### ⚠️ No `processed_at` Audit Column in Any Output
None of Vlad's Silver outputs include a processing timestamp. Without it, there is no way to detect stale data or correlate a row back to a specific pipeline run.

---

## Sergii's Solution (`*_bronze_to_silver.py`, `*_raw_to_silver.py`, `gold_*.py`)

### 📦 Gold Layer Missing Redshift Load (`gold_sales_enriched.py`)
```python
gold.write.mode("overwrite").format("parquet").partitionBy("purchase_date").save(target_s3)
```
The gold job writes enriched data to **S3 as Parquet**. The gold layer is supposed to land in Redshift. The Redshift `COPY` command (or JDBC write, or MERGE) to actually load this data into Redshift is absent. The pipeline is architecturally incomplete — S3 Parquet is staging, not gold.

---

### 🐛 `user_profiles_raw_to_silver.py` — Not Actually a Silver Job
This file reads raw JSON and writes it directly to Parquet with zero transformations:
- `full_name` is not split into `first_name` / `last_name`
- `birth_date` is not cast to `DateType`
- No deduplication by `email` (the downstream join key)

This is a raw → bronze step. Silver layer responsibility — typing, cleaning, validating — is entirely missing. Downstream, the gold join receives untyped profiles and `full_name` is never decomposed, same issue as in Vlad's solution.

---

### ⚠️ Fragile `partition_0` Dependency in `customers_bronze_to_silver.py`
```python
ingest_date = F.when(F.col("partition_0").isNotNull(), parse_ymd("partition_0")).otherwise(F.lit(None))
w = Window.partitionBy("client_id").orderBy(F.col("_ingest_date").desc_nulls_last())
```
The deduplication ordering relies on Glue Crawler automatically naming the S3 partition column `partition_0`. This is undocumented Crawler behavior that breaks if:
- The S3 path structure changes
- The Crawler is re-created or reconfigured
- Data arrives without date-partitioned folder structure

When `partition_0` is null (the `otherwise` branch), `_ingest_date` is null for all rows — the Window ordering becomes non-deterministic and the "keep latest" logic silently fails. A more robust design is to write an explicit `ingested_at` column during the raw → bronze step.

---

### ⚠️ `price` Cast to `double` in `sales_bronze_to_silver.py`
```python
price_num = F.when(...).otherwise(price_clean.cast("double"))
```
Same issue as Vlad — `double` is not suitable for monetary values. Should be `DecimalType(10, 2)`.

---

### ⚠️ Re-Cleaning Silver Data in `gold_sales_enriched.py`
```python
sales2 = sales.select(
    F.trim(F.col("client_id")).alias("client_id"),
    F.col("purchase_date").cast("date"),
    F.col("price").cast("double"),
    ...
)
customers2 = customers.select(
    F.lower(F.trim(F.col("email"))).alias("email"),
    ...
)
```
The gold job re-applies `F.trim()`, `.cast("date")`, `.cast("double")` on data read from Silver tables that should already be clean and typed. If Silver is doing its job, this is unnecessary work. If Silver is not trustworthy enough to skip this, the Silver jobs need to be fixed — not compensated for in Gold.

---

### ⚠️ Dead Parameter `TARGET_TABLE` in `sales_bronze_to_silver.py`
```python
target_table = args["TARGET_TABLE"]  # not used directly, but kept for interface compatibility
```
Same issue as Vlad's `SILVER_DATABASE` — accepted but unused. The comment "kept for interface compatibility" is not a good reason to silently accept a parameter that does nothing.

---

### ⚠️ Silent Column Drops in `user_profiles_raw_to_silver.py`
```python
for field in df.schema.fields:
    if isinstance(field.dataType, NullType):
        df = df.drop(field.name)
```
Columns that are entirely null get silently dropped. If a field like `phone_number` arrives completely empty in a bad batch, the column disappears entirely — the downstream join then fails with a `ColumnNotFound` error far from the root cause. Better to keep null columns and let the pipeline continue, or log an explicit warning with the column name and row counts.

---

### ⚠️ No `processed_at` Audit Column in Any Output
Same issue as Vlad — none of Sergii's Silver outputs include a processing timestamp.

---

## Comparison

| Aspect | Vlad | Sergii |
|--------|------|--------|
| Date normalization | LEGACY mode workaround | Explicit `lpad` normalization ✅ |
| Customer dedup | `.distinct()` — wrong key 🐛 | Window + `partition_0` — fragile ⚠️ |
| `full_name` split | Not done 🐛 | Not done 🐛 |
| `price` type | `double` ⚠️ | `double` ⚠️ |
| Gold layer | Not implemented | S3 only — Redshift missing 📦 |
| Audit column | Missing | Missing |
| Code clarity | Simpler, fewer abstractions | More robust date handling, reusable functions |
| Unused imports | Wildcard `awsglue.transforms` | Clean |
