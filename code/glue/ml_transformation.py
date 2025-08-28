import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = "airbnb-listings268"
input_prefix = "parquet_data/"
output_prefix = "transformed_ml/"

input_path = f"s3://{bucket}/{input_prefix}"
output_path = f"s3://{bucket}/{output_prefix}"

df = spark.read.parquet(input_path)

df = df.withColumn(
    "city",
    F.regexp_extract(F.input_file_name(), r"([^/]+)_listings\.parquet", 1)
)

columns_to_keep = [
    "city",
    "latitude", "longitude", "price", "minimum_nights", "number_of_reviews",
    "last_review", "reviews_per_month", "calculated_host_listings_count",
    "availability_365", "number_of_reviews_ltm"
]
df = df.select(*columns_to_keep)

numeric_cols = [
    "latitude", "longitude", "price", "minimum_nights", "number_of_reviews",
    "reviews_per_month", "calculated_host_listings_count",
    "availability_365", "number_of_reviews_ltm"
]

for col in numeric_cols:
    df = df.withColumn(col, F.col(col).cast("double"))

for col in numeric_cols:
    mean_value = df.select(F.mean(F.col(col))).collect()[0][0]
    df = df.withColumn(col, F.when(F.col(col).isNull(), mean_value).otherwise(F.col(col)))

df = df.withColumn("last_review", F.to_date(F.col("last_review"), "yyyy-MM-dd"))

window = Window.partitionBy("city").orderBy("last_review")
df = df.withColumn("last_review", F.last("last_review", ignorenulls=True).over(window))

df = df.withColumn("lat_lon_product", F.col("latitude") * F.col("longitude"))
df = df.withColumn("reviews_per_availability", F.col("number_of_reviews") / (F.col("availability_365")+1))
df = df.withColumn("reviews_per_min_nights", F.col("number_of_reviews") / (F.col("minimum_nights")+1))

df = df.dropDuplicates()

df.write.mode("overwrite").partitionBy("city").parquet(output_path)

print("Data transformation for ML completed successfully!")

job.commit()