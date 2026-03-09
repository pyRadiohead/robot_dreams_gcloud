import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import NullType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_S3", "TARGET_S3"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

raw_s3 = args["RAW_S3"].rstrip("/") + "/"
target_s3 = args["TARGET_S3"].rstrip("/") + "/"

# Read JSONLines
dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [raw_s3], "recurse": True},
    format="json",
    format_options={"multiline": False},
)

# Convert to DataFrame to safely handle NullType (void)
df = dyf.toDF()

# Drop columns that are entirely null (Spark infers NullType/void)
for field in df.schema.fields:
    if isinstance(field.dataType, NullType):
        df = df.drop(field.name)

# Write to Parquet
(
    df.write.mode("overwrite")
      .format("parquet")
      .save(target_s3)
)

job.commit()