import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

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

# 1. Читаємо дані з Bronze (JSON Lines)
user_profiles_raw = glueContext.create_dynamic_frame.from_catalog(
    database=args['BRONZE_DATABASE'],
    table_name="user_profiles"
).toDF()

# 2. Трансформація (Silver Layer)
# Очікувані колонки: email, full_name, state, birth_date, phone_number
user_profiles_silver = user_profiles_raw.select(
    col("email"),
    col("full_name"),
    col("state"),
    to_date(col("birth_date"), "yyyy-MM-dd").alias("birth_date"),
    col("phone_number")
).distinct()

# 3. Запис у Silver
output_path = f"s3://{args['DATA_LAKE_BUCKET']}/silver/user_profiles/"

user_profiles_silver.write \
    .mode("overwrite") \
    .parquet(output_path)

job.commit()
