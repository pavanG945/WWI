# Databricks notebook source
# MAGIC %pip install boto3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import boto3
import pandas as pd
import io

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp
)

# COMMAND ----------

spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

AWS_ACCESS_KEY="AKIAXYKJXK4S2LVASU7N"

AWS_SECRET_KEY="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"

AWS_REGION="ap-south-1"

BUCKET_NAME="wwi-capstone-ritesh-2026"

S3_PREFIX="June/"

BUSINESS_MONTH = "June 2016"

# COMMAND ----------

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# COMMAND ----------

response = s3.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix=S3_PREFIX
)

csv_files = [
    obj["Key"]
    for obj in response["Contents"]
    if obj["Key"].endswith(".csv")
]

print(f"Total CSV Files : {len(csv_files)}")

for f in csv_files:
    print(f)

# COMMAND ----------

import io
import pandas as pd

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp
)

# ==============================
# Use Catalog & Schema
# ==============================

spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA bronze")

# ==============================
# List June CSV files
# ==============================

response = s3.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix=S3_PREFIX
)

csv_files = [
    obj["Key"]
    for obj in response["Contents"]
    if obj["Key"].endswith(".csv")
]

print(f"Total Files Found : {len(csv_files)}")

# ==============================
# Load Summary
# ==============================

load_summary = []

# ==============================
# Process Every CSV
# ==============================

for file in csv_files:

    print("\n" + "="*100)
    print(f"Processing File : {file}")

    table_name = (
        file.split("/")[-1]
            .replace(".csv", "")
            .lower()
    )

    print(f"Target Table : wwi.bronze.{table_name}")

    try:

        # ----------------------------------
        # Read CSV directly from S3
        # ----------------------------------

        obj = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=file
        )

        pdf = pd.read_csv(
            io.BytesIO(obj["Body"].read())
        )

        source_rows = len(pdf)

        if source_rows == 0:

            print("Empty CSV Found.")

            load_summary.append((
                table_name,
                0,
                0,
                0,
                0,
                "EMPTY"
            ))

            continue

        # ----------------------------------
        # Pandas -> Spark
        # ----------------------------------

        df = spark.createDataFrame(pdf)

        # Convert all column names to lowercase

        df = df.toDF(*[c.lower() for c in df.columns])

        # ----------------------------------
        # Cast all columns to STRING
        # ----------------------------------

        for c in df.columns:

            if c != "ingestion_timestamp":

                df = df.withColumn(
                    c,
                    col(c).cast("string")
                )

        # ----------------------------------
        # Add Metadata Columns if Missing
        # ----------------------------------

        if "business_month" not in df.columns:

            df = df.withColumn(
                "business_month",
                lit(BUSINESS_MONTH)
            )

        if "ingestion_timestamp" not in df.columns:

            df = df.withColumn(
                "ingestion_timestamp",
                current_timestamp()
            )

        # ----------------------------------
        # Read Bronze Table Schema
        # ----------------------------------

        bronze_columns = spark.table(
            f"wwi.bronze.{table_name}"
        ).columns

        bronze_columns = [c.lower() for c in bronze_columns]

        csv_columns = df.columns

        # ----------------------------------
        # Find Missing & Extra Columns
        # ----------------------------------

        missing_columns = [
            c for c in bronze_columns
            if c not in csv_columns
        ]

        extra_columns = [
            c for c in csv_columns
            if c not in bronze_columns
        ]

        print(f"CSV Columns      : {len(csv_columns)}")
        print(f"Bronze Columns   : {len(bronze_columns)}")
        print(f"Missing Columns  : {len(missing_columns)}")
        print(f"Extra Columns    : {len(extra_columns)}")

        if missing_columns:
            print("Missing ->", missing_columns)

        if extra_columns:
            print("Extra ->", extra_columns)

        # ----------------------------------
        # Add Missing Columns
        # ----------------------------------

        for c in missing_columns:

            if c == "business_month":

                df = df.withColumn(
                    c,
                    lit(BUSINESS_MONTH)
                )

            elif c == "ingestion_timestamp":

                df = df.withColumn(
                    c,
                    current_timestamp()
                )

            else:

                df = df.withColumn(
                    c,
                    lit(None).cast("string")
                )

        # ----------------------------------
        # Keep only Bronze Columns
        # ----------------------------------

        df = df.select(bronze_columns)

        # ----------------------------------
        # Insert into Bronze
        # ----------------------------------

        (
            df.write
              .mode("append")
              .insertInto(f"wwi.bronze.{table_name}")
        )

        target_rows = spark.table(
            f"wwi.bronze.{table_name}"
        ).count()

        print(f"Loaded Successfully : {target_rows} rows")

        load_summary.append((
            table_name,
            source_rows,
            target_rows,
            len(missing_columns),
            len(extra_columns),
            "SUCCESS"
        ))

    except Exception as e:

        print(f"FAILED : {e}")

        load_summary.append((
            table_name,
            0,
            0,
            0,
            0,
            f"FAILED : {str(e)}"
        ))

# ==============================
# Validation Report
# ==============================

summary_df = spark.createDataFrame(
    load_summary,
    [
        "table_name",
        "source_rows",
        "bronze_rows",
        "missing_columns",
        "extra_columns",
        "status"
    ]
)
    
display(summary_df)

# COMMAND ----------

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