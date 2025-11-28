StreamPulse – User Engagement 360 Data Pipeline on AWS

This repository contains the implementation of a User Engagement 360 Data Pipeline for the OTT platform StreamPulse.
The system ingests playback logs, content metadata, and user profile data from Amazon S3, processes them using AWS Glue (PySpark), automates workflows through Lambda and EventBridge, stores processed data in Amazon RDS (PostgreSQL), generates daily analytical aggregates to Amazon S3, exposes curated datasets through Amazon Athena, and connects to BI tools such as Power BI or QuickSight for reporting.

The pipeline is fully automated, scalable, and supports both near real-time ingestion and daily batch analytics.

Step 1: S3 Data Lake Setup
Bucket Structure
streampulse-datalake/
    raw/
        playback/
        content_metadata/
        user_profiles/
    analytics/
        daily_aggregates/
    scripts/
        glue/

Data Input

Playback logs → raw/playback/

Content metadata → raw/content_metadata/

User profiles → raw/user_profiles/

Step 2: IAM Roles
Lambda Execution Role

Permissions required:

S3 Read

Glue: StartJobRun

CloudWatch Logs

SecretsManager Read

Glue Job Role

Permissions required:

AmazonS3FullAccess

AmazonRDSFullAccess

SecretsManager Read

CloudWatch Logs

Glue Crawler Role (Optional)

Used if Athena table creation is automated.

Step 3: Lambda Function – Trigger Glue Job 1 (ETL)

This Lambda function is invoked when a new playback file lands in S3.

Responsibilities

Receive S3 event

Build input file path

Trigger Glue ETL Job 1

Log the Glue JobRunId

Trigger

S3 Event Notification configured for prefix:

raw/playback/

Step 4: AWS Glue Job 1 – ETL and Load to PostgreSQL

This Spark-based Glue job performs data cleaning and transformation.

Responsibilities

Read playback logs, content metadata, and user profiles

Parse and clean timestamps

Compute duration_min

Join all datasets

Produce 11 standardized columns

Load processed data into RDS table:

processed.processed_data

Step 5: Amazon RDS PostgreSQL Setup
Configuration

Engine: PostgreSQL

DB name: streampulse

Instance type: db.t3.micro

Security group configured for allowed access

Connectivity

Connect with pgAdmin, DBeaver, or SQL client using:

Endpoint

Port

Username

Password

Step 6: EventBridge Schedule – Trigger Aggregation

A scheduled EventBridge rule runs Glue Job 2 every day at 2:00 AM IST.

Schedule Rule
cron(30 20 * * ? *)   # 2 AM IST

Target

Lambda function that triggers Glue Job 2

Step 7: Lambda Function – Trigger Glue Job 2
Responsibilities

Trigger the daily aggregation Glue job

Pass output S3 path and Secrets ARN as arguments

Trigger

EventBridge daily schedule

Step 8: AWS Glue Job 2 – Daily Aggregation

This Glue job computes aggregated engagement metrics for analytics.

Responsibilities

Read processed data from RDS

Group by:

Date

Region

Device type

Content

Compute metrics:

Unique viewers

Total watch minutes

Total watch hours

Average session duration

Write output to:

s3://streampulse-datalake/analytics/daily_aggregates/year=YYYY/month=MM/day=DD/

Step 9: Athena Configuration

Athena is used to query the curated aggregated dataset.

Setup Steps

Configure Athena query result location

Create Glue Crawler for:

s3://streampulse-datalake/analytics/daily_aggregates/


Generate table under Glue Data Catalog

Sample Query
SELECT 
    title,
    total_watch_hours,
    unique_viewers
FROM daily_engagement
ORDER BY total_watch_hours DESC
LIMIT 10;

Step 10: Dashboards (Power BI / QuickSight)
Example Dashboards

Top 10 most watched titles

Daily unique viewers

Watch hours by device

Regional engagement insights

Genre performance

Daily active users

Connectivity

Power BI → Athena ODBC
