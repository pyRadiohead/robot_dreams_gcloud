import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, regexp_replace

# Отримуємо параметри
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_DATABASE', 'SILVER_DATABASE', 'DATA_LAKE_BUCKET'])

sc = SparkContext()
# Налаштовуємо Spark бути лояльним до старих форматів дат
sc._jsc.hadoopConfiguration().set("spark.sql.legacy.timeParserPolicy", "LEGACY")

glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Також встановлюємо через spark config для впевненості
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Читаємо дані з Bronze
# Примітка: Glue Crawler за замовчуванням приводить назви колонок до нижнього регістру
sales_raw = glueContext.create_dynamic_frame.from_catalog(
    database=args['BRONZE_DATABASE'],
    table_name="sales"
).toDF()

# 2. Очищення та трансформація
# - Видаляємо '$' та інші нецифрові символи з ціни
# - Замінюємо '/' на '-' у датах для стабільного парсингу
sales_silver = sales_raw.withColumn("clean_price", regexp_replace(col("price"), "[^0-9.]", "")) \
    .withColumn("clean_date", regexp_replace(col("purchasedate"), "/", "-")) \
    .select(
        col("customerid").alias("client_id"),
        to_date(col("clean_date"), "yyyy-MM-dd").alias("purchase_date"),
        col("product").alias("product_name"),
        col("clean_price").cast("double").alias("price")
    ).dropna(subset=["client_id", "purchase_date"])

# 3. Запис у Silver (Parquet формат, партиціонування по даті)
output_path = f"s3://{args['DATA_LAKE_BUCKET']}/silver/sales/"

sales_silver.write \
    .mode("overwrite") \
    .partitionBy("purchase_date") \
    .parquet(output_path)

job.commit()