import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "DATABASE", "SOURCE_TABLE", "TARGET_S3", "TARGET_TABLE"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

db = args["DATABASE"]
src = args["SOURCE_TABLE"]
target_s3 = args["TARGET_S3"].rstrip("/") + "/"
target_table = args["TARGET_TABLE"]  # not used directly, but kept for interface compatibility

# Read from Glue Catalog (bronze)
df = spark.table(f"`{db}`.{src}")


def normalize_yyyy_m_d(col):
    """
    Normalizes strings like:
      - '2022-9-1'   -> '2022-09-01'
      - '2022-09-1'  -> '2022-09-01'
      - '2022-09-01' -> '2022-09-01'
    Also safely handles values with time part:
      - '2022-09-01 12:34:56' -> '2022-09-01'
      - '2022-09-01T12:34:56' -> '2022-09-01'
    """
    # trim and take only date-like part before space or 'T'
    s = F.trim(col)
    s = F.split(s, r"[T\s]+").getItem(0)

    parts = F.split(s, "-")
    # Expecting at least 3 parts, but if not, will produce nulls
    y = parts.getItem(0)
    m = F.lpad(parts.getItem(1), 2, "0")
    d = F.lpad(parts.getItem(2), 2, "0")

    return F.concat_ws("-", y, m, d)


# --------------------------
# Clean + transform
# --------------------------
purchase_date_str = normalize_yyyy_m_d(F.col("purchasedate"))
purchase_date = F.to_date(purchase_date_str, "yyyy-MM-dd")

price_clean = F.regexp_replace(F.col("price"), r"[^0-9\.\,]", "")
price_clean = F.regexp_replace(price_clean, ",", ".")
price_num = F.when(F.trim(price_clean) == "", None).otherwise(price_clean.cast("double"))

out = (
    df.select(
        F.trim(F.col("customerid")).alias("client_id"),
        purchase_date.alias("purchase_date"),
        F.trim(F.col("product")).alias("product_name"),
        price_num.alias("price"),
    )
    .where(F.col("client_id").isNotNull() & F.col("purchase_date").isNotNull())
)

# Write parquet partitioned by purchase_date
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    out.write.mode("overwrite")
    .format("parquet")
    .partitionBy("purchase_date")
    .save(target_s3)
)

job.commit()