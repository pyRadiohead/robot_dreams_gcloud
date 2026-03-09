import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

df = spark.table(f"`{db}`.`{src}`")

def normalize_ymd_str(colname: str):
    """
    нормалізує '2022-08-1' або '2022-8-01' -> '2022-08-01'
    якщо формат інший/порожній -> null
    """
    s = F.trim(F.col(colname))
    # витягаємо y, m, d через regex
    y = F.regexp_extract(s, r"^(\d{4})-", 1)
    m = F.regexp_extract(s, r"^\d{4}-(\d{1,2})-", 1)
    d = F.regexp_extract(s, r"^\d{4}-\d{1,2}-(\d{1,2})$", 1)

    # якщо не матчиться — будуть пусті строки -> зробимо null
    m2 = F.when(m == "", None).otherwise(F.lpad(m, 2, "0"))
    d2 = F.when(d == "", None).otherwise(F.lpad(d, 2, "0"))
    y2 = F.when(y == "", None).otherwise(y)

    return F.concat_ws("-", y2, m2, d2)

def parse_ymd(colname: str):
    return F.to_date(normalize_ymd_str(colname), "yyyy-MM-dd")

registration_date = parse_ymd("registrationdate")

# partition_0 може бути колонкою (інколи crawler так додає)
ingest_date = F.when(F.col("partition_0").isNotNull(), parse_ymd("partition_0")).otherwise(F.lit(None))

out0 = (
    df.select(
        F.trim(F.col("id")).alias("client_id"),
        F.trim(F.col("firstname")).alias("first_name"),
        F.trim(F.col("lastname")).alias("last_name"),
        F.trim(F.col("email")).alias("email"),
        registration_date.alias("registration_date"),
        F.trim(F.col("state")).alias("state"),
        ingest_date.alias("_ingest_date"),
    )
    .where(F.col("client_id").isNotNull() & (F.col("client_id") != ""))
)

# customers приходить повним дампом щодня -> беремо найсвіжіший запис по client_id
w = Window.partitionBy("client_id").orderBy(F.col("_ingest_date").desc_nulls_last())

out = (
    out0.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn", "_ingest_date")
)

(
    out.write.mode("overwrite")
       .format("parquet")
       .save(target_s3)
)

job.commit()