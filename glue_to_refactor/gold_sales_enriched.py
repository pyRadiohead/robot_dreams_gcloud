import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "DATABASE", "SALES_TABLE", "CUSTOMERS_TABLE", "PROFILES_TABLE", "TARGET_S3"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

db = args["DATABASE"]
sales_tbl = args["SALES_TABLE"]
cust_tbl = args["CUSTOMERS_TABLE"]
prof_tbl = args["PROFILES_TABLE"]
target_s3 = args["TARGET_S3"].rstrip("/") + "/"

# Read silver tables from Glue Catalog
sales = spark.table(f"`{db}`.`{sales_tbl}`")
customers = spark.table(f"`{db}`.`{cust_tbl}`")
profiles = spark.table(f"`{db}`.`{prof_tbl}`")

# ---------- Clean / normalize ----------
# sales
sales2 = (
    sales.select(
        F.trim(F.col("client_id")).alias("client_id"),
        F.col("purchase_date").cast("date").alias("purchase_date"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.col("price").cast("double").alias("price"),
    )
    .where(F.col("client_id").isNotNull() & F.col("purchase_date").isNotNull())
)

# customers
customers2 = customers.select(
    F.trim(F.col("client_id")).alias("client_id"),
    F.trim(F.col("first_name")).alias("first_name"),
    F.trim(F.col("last_name")).alias("last_name"),
    F.lower(F.trim(F.col("email"))).alias("email"),
    F.col("registration_date").cast("date").alias("registration_date"),
    F.trim(F.col("state")).alias("customer_state"),
)

# profiles
profiles2 = profiles.select(
    F.lower(F.trim(F.col("email"))).alias("email"),
    F.trim(F.col("full_name")).alias("full_name"),
    F.trim(F.col("state")).alias("profile_state"),
    F.col("birth_date").cast("date").alias("birth_date"),
    F.trim(F.col("phone_number")).alias("phone_number"),
)

# If profiles може містити дублікати по email — заберемо один запис на email
profiles2 = profiles2.dropDuplicates(["email"])

# ---------- Join ----------
gold = (
    sales2.alias("s")
    .join(customers2.alias("c"), on="client_id", how="left")
    .join(profiles2.alias("p"), on="email", how="left")
    .select(
        F.col("s.purchase_date").alias("purchase_date"),
        F.col("client_id"),
        F.col("c.first_name"),
        F.col("c.last_name"),
        F.col("email"),
        F.coalesce(F.col("p.profile_state"), F.col("c.customer_state")).alias("state"),
        F.col("c.registration_date"),
        F.col("p.full_name"),
        F.col("p.birth_date"),
        F.col("p.phone_number"),
        F.col("s.product_name"),
        F.col("s.price"),
    )
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    gold.write.mode("overwrite")
    .format("parquet")
    .partitionBy("purchase_date")
    .save(target_s3)
)

job.commit()