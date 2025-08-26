import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = "airbnb-listings268"
input_prefix = "parquet_data/"
output_prefix = "transformed_bi/"

input_path = f"s3://{bucket}/{input_prefix}"
output_path = f"s3://{bucket}/{output_prefix}"

columns_bi = [
    "id","name","latitude","longitude","room_type","price","minimum_nights",
    "number_of_reviews","last_review","reviews_per_month",
    "calculated_host_listings_count","availability_365","number_of_reviews_ltm"
]

df = spark.read.parquet(input_path)

df = df.withColumn(
    "city",
    F.regexp_extract(F.input_file_name(), r"([^/]+)_listings\.parquet", 1)
)

df = df.select(columns_bi + ["city"])

df = df.dropDuplicates(["id", "host_id"] if "host_id" in df.columns else ["id"])

numeric_cols = ["price","minimum_nights","number_of_reviews","reviews_per_month",
                "calculated_host_listings_count","availability_365","number_of_reviews_ltm"]

for col in numeric_cols:
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast("double"))

for col in numeric_cols:
    if col in df.columns:
        mean_val = df.select(F.mean(F.col(col))).collect()[0][0]
        if mean_val is None:
            mean_val = 0
        df = df.fillna({col: mean_val})

categorical_cols = ["name","room_type","last_review"]
for col in categorical_cols:
    if col in df.columns:
        mode_row = df.groupBy(col).count().orderBy(F.desc("count")).limit(1).collect()
        mode_val = "Unknown"
        if mode_row:
            val = mode_row[0][col]
            if val is not None and str(val).strip() != "":
                mode_val = str(val)
        df = df.fillna({col: mode_val})

df.write.mode("overwrite").partitionBy("city").parquet(output_path)

print("Data cleaning for BI completed successfully!")


job.commit()