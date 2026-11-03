# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC #### Task 9: Set up Bronze notebook - use WWI Catalog and Bronze schema

# COMMAND ----------

# T9 - Import Required Libraries
import os
import io
import boto3
import pandas as pd

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp
)

# COMMAND ----------

# T9 - Use WWI Catalog and bronze Schema
spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

# T9 - Verify Catalog
spark.sql("SELECT current_catalog()").show()

# COMMAND ----------

# T9 - Verify Schema
spark.sql("SELECT current_database()").show()

# COMMAND ----------

# T9 - List Existing Bronze Tables
spark.sql("SHOW TABLES IN wwi.bronze").show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 10: Load environment configuration - define AWS region, bucket, S3 prefix, temp and volume storage paths

# COMMAND ----------

# ==========================================
# T10 - Environment Configuration
# ==========================================

# AWS Configuration
AWS_REGION = "ap-south-1"
AWS_ACCESS_KEY = "AKIAXYKJXK4S2LVASU7N"
AWS_SECRET_KEY = "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"

# S3 Configuration
BUCKET_NAME = "wwi-capstone-ritesh-2026"
S3_PREFIX = "August/"          # Folder to process

# Business Information
BUSINESS_MONTH = "2016-08"

# Local Temporary Directory
TEMP_DIR = "/tmp/wwi_bronze"

# Databricks Configuration
CATALOG_NAME = "wwi"
SCHEMA_NAME = "bronze"

# COMMAND ----------

# T10 - Verify Configuration
print("=" * 60)
print("Environment Configuration")
print("=" * 60)

print(f"Catalog           : {CATALOG_NAME}")
print(f"Schema            : {SCHEMA_NAME}")
print(f"AWS Region        : {AWS_REGION}")
print(f"S3 Bucket         : {BUCKET_NAME}")
print(f"S3 Folder         : {S3_PREFIX}")
print(f"Business Month    : {BUSINESS_MONTH}")
print(f"Temp Directory    : {TEMP_DIR}")

# COMMAND ----------

# T10 - Create Temporary Directory
import os

os.makedirs(TEMP_DIR, exist_ok=True)

print(f"Temporary directory created: {TEMP_DIR}")

# COMMAND ----------

# T10 - Verify Directory
os.path.exists(TEMP_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 11: Initialize S3 client using boto3 - create client, configure authentication keys and region using AWS S3 credentials

# COMMAND ----------

# T11 Initialize S3 Client using boto3
import boto3

# COMMAND ----------

# ==========================================
# T11 - Initialize S3 Client
# ==========================================

s3 = boto3.client(
    service_name="s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# COMMAND ----------

# T11 - Verify AWS Connection
try:

    response = s3.list_buckets()

    print("AWS Connection Successful")
    
    print("Available Buckets:")

    for bucket in response["Buckets"]:
        print("-", bucket["Name"])

except Exception as e:

    print("Connection Failed")
    print(e)

# COMMAND ----------

# T11 - Verify the Required Bucket
try:

    s3.head_bucket(Bucket=BUCKET_NAME)

    print(f"Bucket '{BUCKET_NAME}' exists and is accessible.")

except Exception as e:

    print("Bucket not found or access denied.")
    print(e)

# COMMAND ----------

# T11 - Display Configuration
print("="*60)

print("S3 Client Initialized Successfully")

print("="*60)

print(f"Bucket Name    : {BUCKET_NAME}")
print(f"AWS Region     : {AWS_REGION}")
print(f"Folder         : {S3_PREFIX}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 12: List files from S3 - scan S3 folder for June data, collect all files under S3 bucket

# COMMAND ----------

# ==========================================
# T12 - List Objects from S3
# ==========================================

response = s3.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix=S3_PREFIX
)

# COMMAND ----------

# ==========================================
# T12 - Collect CSV Files
# ==========================================

csv_files = [
    obj["Key"]
    for obj in response.get("Contents", [])
    if obj["Key"].endswith(".csv")
]

print(f"Total CSV Files Found : {len(csv_files)}")

# COMMAND ----------

# ==========================================
# T12 - Display CSV Files
# ==========================================

print("=" * 80)
print("CSV Files Found in S3")
print("=" * 80)

for i, file in enumerate(csv_files, start=1):
    print(f"{i:02d}. {file}")

# COMMAND ----------

# ==========================================
# T12 - Validate Expected File Count
# ==========================================

EXPECTED_FILES = 31

if len(csv_files) == EXPECTED_FILES:
    print(f"SUCCESS: {EXPECTED_FILES} CSV files found.")
else:
    print(f"WARNING: Expected {EXPECTED_FILES} files, but found {len(csv_files)}.")

# COMMAND ----------

# ==========================================
# T12 - Store File List for ETL
# ==========================================

bronze_file_list = csv_files

print("Bronze File List Created Successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 13: Download filtered S3 files to temp directory, preserve original file names

# COMMAND ----------

# ==========================================
# T13 - Download CSV Files from S3
# ==========================================

downloaded_files = []
failed_files = []

for file in csv_files:

    try:

        # Extract file name
        filename = os.path.basename(file)

        # Local destination
        local_file_path = os.path.join(TEMP_DIR, filename)

        # Download file
        s3.download_file(
            Bucket=BUCKET_NAME,
            Key=file,
            Filename=local_file_path
        )

        downloaded_files.append(local_file_path)

        print(f"Downloaded : {filename}")

    except Exception as e:

        failed_files.append(file)

        print(f"Failed : {file}")
        print(e)

# COMMAND ----------

# ==========================================
# T13 - Verify Downloaded Files
# ==========================================

print("="*70)
print("Downloaded Files")
print("="*70)

for file in downloaded_files:
    print(file)

print("\nTotal Downloaded :", len(downloaded_files))

# COMMAND ----------

# ==========================================
# T13 - Verify Files in Temporary Directory
# ==========================================

temp_files = os.listdir(TEMP_DIR)

print(f"Files Available in Temp Directory : {len(temp_files)}")

for file in sorted(temp_files):
    print(file)

# COMMAND ----------

# ==========================================
# T13 - Download Summary
# ==========================================

print("="*70)
print("Download Summary")
print("="*70)

print(f"Total Files Expected  : {len(csv_files)}")
print(f"Downloaded Successfully : {len(downloaded_files)}")
print(f"Failed Downloads : {len(failed_files)}")

if failed_files:
    print("\nFailed Files:")
    for file in failed_files:
        print(file)
else:
    print("\nAll files downloaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 14: Copy files from temp directory to Databricks volume path, ensure files are ready for Spark access

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG wwi;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS rawfiles;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN wwi.bronze;

# COMMAND ----------

# ==========================================
# T14 - Databricks Volume Path
# ==========================================

VOLUME_PATH = "/Volumes/wwi/bronze/rawfiles"

print(f"Volume Path : {VOLUME_PATH}")

# COMMAND ----------

# T14 - Create the Volume Directory
dbutils.fs.mkdirs(VOLUME_PATH)

print("Volume directory created successfully.")

# COMMAND ----------

# T14 - Copy Files from Temp Directory to Volume
import os

copied_files = []
failed_files = []

for file in os.listdir(TEMP_DIR):

    try:

        source = f"file:{os.path.join(TEMP_DIR, file)}"
        destination = f"{VOLUME_PATH}/{file}"

        dbutils.fs.cp(source, destination)

        copied_files.append(file)

        print(f"Copied : {file}")

    except Exception as e:

        failed_files.append(file)

        print(f"Failed : {file}")
        print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 15: Read CSV files safely - read using Pandas to handle empty CSV files, convert to Spark DataFrame, cast all columns to STRING to preserve original data

# COMMAND ----------

# T15 - Read CSV Files Safely

dataframes = {}

for file in csv_files:

    print("=" * 80)
    print(f"Reading File : {file}")

    try:

        # Read file from S3
        obj = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=file
        )

        # Read using Pandas
        pdf = pd.read_csv(
            io.BytesIO(obj["Body"].read())
        )

        # Handle empty CSV
        if pdf.empty:

            print("Empty CSV File")
            continue

        # Convert Pandas -> Spark
        df = spark.createDataFrame(pdf)

        # Convert column names to lowercase
        df = df.toDF(*[c.lower() for c in df.columns])

        # Cast every column to STRING
        for c in df.columns:

            df = df.withColumn(
                c,
                col(c).cast("string")
            )

        # Generate Bronze table name
        table_name = (
            file.split("/")[-1]
                .replace(".csv", "")
                .lower()
        )

        # Store DataFrame for later tasks
        dataframes[table_name] = df

        print(f"Table Name      : {table_name}")
        print(f"Rows            : {df.count()}")
        print(f"Columns         : {len(df.columns)}")

    except Exception as e:

        print(f"Failed : {file}")
        print(e)

# COMMAND ----------

# T15 - Verify Loaded DataFrames
print("=" * 80)
print("Loaded Bronze DataFrames")
print("=" * 80)

for table in dataframes.keys():
    print(table)

# COMMAND ----------

# T15 - wwi.bronze.application_citiesPreview One DataFrame
display(dataframes["application_cities"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 16: Add ingestion metadata - add business_month column and ingestion_timestamp column

# COMMAND ----------

# T16 - Add Metadata Columns

for table_name, df in dataframes.items():

    # Add business_month if missing
    if "business_month" not in df.columns:

        df = df.withColumn(
            "business_month",
            lit(BUSINESS_MONTH)
        )

    # Add ingestion_timestamp if missing
    if "ingestion_timestamp" not in df.columns:

        df = df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )

    # Update dictionary
    dataframes[table_name] = df

    print(f"Metadata added -> {table_name}")

# COMMAND ----------

# T16 - Verify Metadata
display(
    dataframes["application_cities"]
)

# COMMAND ----------

# T16 - Verify Schema
dataframes["application_cities"].printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 17: Generate Bronze table names - remove .csv extension, convert to lowercase for consistent naming

# COMMAND ----------

# T17 - Generate Bronze Table Names

table_mapping = {}

# Month suffixes to remove
month_suffixes = [
    "_january", "_february", "_march", "_april", "_may", "_june",
    "_july", "_august", "_september", "_october", "_november", "_december"
]

for file in csv_files:

    # Extract only the file name
    file_name = file.split("/")[-1]

    # Remove .csv extension
    table_name = file_name.replace(".csv", "")

    # Convert to lowercase
    table_name = table_name.lower()

    # Remove month suffixes if present
    for suffix in month_suffixes:
        if table_name.endswith(suffix):
            table_name = table_name.replace(suffix, "")
            break

    # Store mapping
    table_mapping[file] = table_name

# COMMAND ----------

# ==========================================
# T17 - Display CSV -> Bronze Table Mapping
# ==========================================

print("=" * 90)
print("CSV File".ljust(50), "Bronze Table")
print("=" * 90)

for file, table in table_mapping.items():
    print(file.ljust(50), table)

# COMMAND ----------

# T17 - Verify Total Tables
print(f"Total CSV Files   : {len(csv_files)}")
print(f"Total Bronze Tables Generated : {len(table_mapping)}")

# COMMAND ----------

# T17 - Verify Tables Exist in Bronze Schema
bronze_tables = [
    row.tableName.lower()
    for row in spark.sql("SHOW TABLES IN wwi.bronze").collect()
]

missing_tables = []

for table in table_mapping.values():

    if table not in bronze_tables:
        missing_tables.append(table)

if len(missing_tables) == 0:
    print("All Bronze tables exist.")
else:
    print("Missing Bronze Tables:")
    for table in missing_tables:
        print(table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 18: Write data as Delta tables under wwi.bronze schema, do final row count after loading

# COMMAND ----------

# ==========================================
# T18 - Bronze Load Summary
# ==========================================

load_summary = []

# COMMAND ----------

# ==========================================
# T18 - Load Bronze Tables
# ==========================================

from pyspark.sql.functions import lit, current_timestamp

for file, table_name in table_mapping.items():

    print("=" * 100)
    print(f"Processing : {file}")
    print(f"Target Table : wwi.bronze.{table_name}")

    try:

        # Read file from S3
        obj = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=file
        )

        pdf = pd.read_csv(
            io.BytesIO(obj["Body"].read())
        )

        source_rows = len(pdf)

        # Handle empty file
        if source_rows == 0:

            print("Empty CSV File")

            load_summary.append((
                table_name,
                0,
                0,
                "EMPTY"
            ))

            continue

        # Pandas -> Spark
        df = spark.createDataFrame(pdf)

        # Lowercase column names
        df = df.toDF(*[c.lower() for c in df.columns])

        # Cast every column to STRING
        for c in df.columns:
            df = df.withColumn(c, col(c).cast("string"))

        # Metadata
        df = (
            df
            .withColumn("business_month", lit(BUSINESS_MONTH))
            .withColumn("ingestion_timestamp", current_timestamp())
        )

        # Read Bronze schema
        bronze_columns = spark.table(
            f"wwi.bronze.{table_name}"
        ).columns

        bronze_columns = [c.lower() for c in bronze_columns]

        # Add missing columns
        for column in bronze_columns:

            if column not in df.columns:

                if column == "business_month":

                    df = df.withColumn(
                        column,
                        lit(BUSINESS_MONTH)
                    )

                elif column == "ingestion_timestamp":

                    df = df.withColumn(
                        column,
                        current_timestamp()
                    )

                else:

                    df = df.withColumn(
                        column,
                        lit(None).cast("string")
                    )

        # Keep only Bronze columns
        df = df.select(bronze_columns)

        # Write to Bronze
        (
            df.write
              .mode("append")
              .insertInto(f"wwi.bronze.{table_name}")
        )

        target_rows = spark.table(
            f"wwi.bronze.{table_name}"
        ).count()

        load_summary.append((
            table_name,
            source_rows,
            target_rows,
            "SUCCESS"
        ))

        print(f"Loaded Successfully : {target_rows}")

    except Exception as e:

        load_summary.append((
            table_name,
            0,
            0,
            f"FAILED : {str(e)}"
        ))

        print(e)

# COMMAND ----------

# T18 - Load Summary Report
summary_df = spark.createDataFrame(

    load_summary,

    [
        "table_name",
        "source_rows",
        "bronze_rows",
        "status"
    ]

)

display(summary_df)

# COMMAND ----------

# T18 - Validate All Bronze Tables
validation = []

tables = spark.sql(
    "SHOW TABLES IN wwi.bronze"
).collect()

for table in tables:

    count = spark.table(
        f"wwi.bronze.{table.tableName}"
    ).count()

    validation.append(
        (
            table.tableName,
            count
        )
    )

validation_df = spark.createDataFrame(
    validation,
    [
        "table_name",
        "row_count"
    ]
)

display(validation_df)

# COMMAND ----------

# T18 - Verify One Table
display(
    spark.table("wwi.bronze.application_cities").count()
)

# COMMAND ----------

