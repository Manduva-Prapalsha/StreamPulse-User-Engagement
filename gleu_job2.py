#glue_Script2
import sys
import json
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, countDistinct, sum as _sum, avg, round

# Job Arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SECRET_ARN', 'OUTPUT_S3_PATH'])
JOB_NAME = args['JOB_NAME']
SECRET_ARN = args['SECRET_ARN']
BASE_OUTPUT_S3_PATH = args['OUTPUT_S3_PATH']  

# Initialize Spark & Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# Retrieve DB credentials
secrets_client = boto3.client('secretsmanager')
secret_value = secrets_client.get_secret_value(SecretId=SECRET_ARN)
db_secrets = json.loads(secret_value['SecretString'])

db_host = db_secrets['host']
db_port = db_secrets['port']
db_name = "postgres"
db_user = db_secrets['username']
db_pass = db_secrets['password']

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
source_table = "processed.processed_data"

# Read from processed_data table
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", source_table) \
    .option("user", db_user) \
    .option("password", db_pass) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Aggregation
agg_df = (
    df.groupBy("date", "region", "device_type", "content_id", "title", "genre", "language")
      .agg(
          countDistinct("user_id").alias("unique_viewers"),
          _sum("duration_min").alias("total_watch_minutes"),
          round(_sum("duration_min") / 60, 2).alias("total_watch_hours"),
          round(avg("duration_min"), 2).alias("avg_session_duration")
      )
)

# todays date for output folder
today = datetime.utcnow()
year = today.year
month = today.month
day = today.day

# Construct dynamic output path
final_output_path = f"{BASE_OUTPUT_S3_PATH}year={year}/month={month:02d}/day={day:02d}/"

# sample
print("Sample aggregated data:")
for row in agg_df.limit(5).collect():
    print(row)

# write output to S3
(
    agg_df.write
        .mode("overwrite")
        .parquet(final_output_path)
)

print(f"data successfully written to {final_output_path} in Parquet format")

job.commit()
