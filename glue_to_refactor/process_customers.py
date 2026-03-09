import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Отримуємо параметри
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_DATABASE', 'SILVER_DATABASE', 'DATA_LAKE_BUCKET'])

sc = SparkContext()
# Налаштовуємо лояльність до форматів дат
sc._jsc.hadoopConfiguration().set("spark.sql.legacy.timeParserPolicy", "LEGACY")

glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Читаємо дані з Bronze
customers_raw = glueContext.create_dynamic_frame.from_catalog(
    database=args['BRONZE_DATABASE'],
    table_name="customers"
).toDF()

# 2. Трансформація
# Використовуємо нижній регістр для вхідних колонок (customerid, firstname і т.д.)
customers_silver = customers_raw.select(
    col("id").alias("client_id"),
    col("firstname").alias("first_name"),
    col("lastname").alias("last_name"),
    col("email").alias("email"),
    col("registrationdate").alias("registration_date"),
    col("state").alias("state")
).distinct()

# 3. Запис у Silver
output_path = f"s3://{args['DATA_LAKE_BUCKET']}/silver/customers/"

customers_silver.write \
    .mode("overwrite") \
    .parquet(output_path)

job.commit()
