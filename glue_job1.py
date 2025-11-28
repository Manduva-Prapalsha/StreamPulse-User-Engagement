import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp, to_date, lit

# Read arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'file_path', 'SECRET_ARN'])

JOB_NAME = args['JOB_NAME']
file_path = args['file_path']
SECRET_ARN = args['SECRET_ARN']

print(f"Starting Glue Job: {JOB_NAME}")
print(f"Reading playback file from: {file_path}")

# Initialize Glue and Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# credentials from Secrets Manager
secrets_client = boto3.client('secretsmanager')
secret_value = secrets_client.get_secret_value(SecretId=SECRET_ARN)
db_secrets = json.loads(secret_value['SecretString'])

db_host = db_secrets['host']
db_port = db_secrets['port']
db_name = "postgres" 
db_user = db_secrets['username']
db_pass = db_secrets['password']

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
db_table = "processed.processed_data"

print(f"Connected to DB: {db_name} on {db_host}")

#Read data from S3
playback_df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
content_df = spark.read.option("header", True).option("inferSchema", True).csv("s3://streampulse-data/raw1/content_metadata/")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("s3://streampulse-data/raw1/user_profile/")

print("All input datasets loaded successfully")

# Perform joins

users_prefixed_df = users_df.select([col(c).alias(f"user_{c}") for c in users_df.columns])

join1_df = playback_df.join(content_df, on="content_id", how="inner")
final_df = join1_df.join(users_prefixed_df, join1_df["user_id"] == users_prefixed_df["user_user_id"], how="inner")

final_df = final_df.drop("user_user_id")

#user_region as region
region_like_cols = [c for c in final_df.columns if "region" in c.lower() and c != "user_region"]
for c in region_like_cols:
    final_df = final_df.drop(c)

if "user_region" in final_df.columns:
    final_df = final_df.withColumnRenamed("user_region", "region")

print("Joins completed successfully")

# transformations
final_df = final_df.withColumn("start_time", to_timestamp(col("start_time"), "dd-MM-yyyy HH:mm"))
final_df = final_df.withColumn("end_time", to_timestamp(col("end_time"), "dd-MM-yyyy HH:mm"))
final_df = final_df.withColumn("duration_min", (col("end_time").cast("long") - col("start_time").cast("long")) / 60)
final_df = final_df.withColumn("date", to_date(col("start_time")))

# Drop duplicates and clean up
final_df = final_df.dropDuplicates()

# Explicitly select the required 11 columns in order
final_df = final_df.select(
    "user_id",
    "content_id",
    "title",
    "genre",
    "language",
    "region",
    "device_type",
    "start_time",
    "end_time",
    "duration_min",
    "date"
)

print("Data transformed and aligned with RDS schema")

#Write to Postgresql

jdbc_properties = {
    "user": db_user,
    "password": db_pass,
    "driver": "org.postgresql.Driver"
}

print("Writing processed data into PostgreSQL RDS table...")

final_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", db_table) \
    .option("user", db_user) \
    .option("password", db_pass) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print(f"Data written successfully to RDS table: {db_table}")

job.commit()
print("Glue Job completed successfully.")
