# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_Common_Functions

# COMMAND ----------

dbutils.widgets.text(
    "business_month",
    "2016-06",
    "Business Month"
)

business_month = dbutils.widgets.get("business_month").strip()

if business_month == "":
    raise Exception("Business Month is Mandatory")

print(f"Business Month : {business_month}")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

# Bronze Table
bronze_paymentmethod_table = "wwi.bronze.application_paymentmethods"

# Silver Table
silver_paymentmethod_table = "wwi.silver.paymentmethod_staging"

# Central Logger
dq_logger_table = "wwi.logger.dq_logger"

# Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("Pipeline Run ID :", pipeline_run_id)


# COMMAND ----------

verify_logger_table(dq_logger_table)

# COMMAND ----------

spark.table(dq_logger_table).printSchema()

display(
    spark.table(dq_logger_table).limit(5)
)

# COMMAND ----------

print(log_dq)

# COMMAND ----------

# ==========================================
# Task 5 : Load Bronze PaymentMethod Table
# ==========================================

paymentmethod_df = (
    spark.table(bronze_paymentmethod_table)
    .filter(col("business_month") == business_month)
)

# Validate Input Data

print("Payment Method Records :",
      paymentmethod_df.count())

if paymentmethod_df.count() == 0:
    raise Exception(
        "No Payment Method records found for selected Business Month."
    )

# COMMAND ----------

# Verify Schema

paymentmethod_df.printSchema()

# COMMAND ----------

# Verify Sample Data

display(paymentmethod_df.limit(10))

# COMMAND ----------

# ==========================================
# Task 6 : Standardize Payment Method Data
# ==========================================

from pyspark.sql.functions import *

paymentmethod_df = (
    paymentmethod_df

    # ==========================
    # Cast ID Columns
    # ==========================
    .withColumn(
        "paymentmethodid",
        col("paymentmethodid").cast("double").cast("int")
    )

    .withColumn(
        "lasteditedby",
        col("lasteditedby").cast("double").cast("int")
    )

    # ==========================
    # Trim Text Columns
    # ==========================
    .withColumn(
        "paymentmethodname",
        trim(col("paymentmethodname"))
    )

    # ==========================
    # Title Case
    # ==========================
    .withColumn(
        "paymentmethodname",
        initcap(col("paymentmethodname"))
    )

    # ==========================
    # Convert Date Columns
    # ==========================
    .withColumn(
        "validfrom",
        to_timestamp(col("validfrom"), "dd-MM-yyyy HH:mm:ss")
    )

    .withColumn(
        "validto",
        to_timestamp(col("validto"), "dd-MM-yyyy HH:mm:ss")
    )

    # ==========================
    # Convert Ingestion Timestamp
    # ==========================
    .withColumn(
        "ingested_timestamp",
        to_timestamp(col("ingested_timestamp"))
    )
)

# COMMAND ----------

display(
    paymentmethod_df.select(
        "paymentmethodid",
        "paymentmethodname",
        "validfrom",
        "validto",
        "lasteditedby",
        "business_month",
        "ingested_timestamp"
    ).limit(10)
)

# COMMAND ----------

# ==========================================
# Task 7 : Pre-Join DQ Checks
# ==========================================

total_records = paymentmethod_df.count()

print("Total Payment Method Records :", total_records)

# COMMAND ----------


null_paymentmethodid = paymentmethod_df.filter(
    col("paymentmethodid").isNull()
).count()

print("NULL Payment Method ID :", null_paymentmethodid)

log_dq(
    table_name="application_paymentmethods",
    column_name="paymentmethodid",
    dq_type="NULL_CHECK",
    dq_description="Payment Method ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Primary Key NULL Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_paymentmethodid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

invalid_paymentmethodid = paymentmethod_df.filter(
    col("paymentmethodid") <= 0
).count()

print("Invalid Payment Method ID :", invalid_paymentmethodid)

log_dq(
    table_name="application_paymentmethods",
    column_name="paymentmethodid",
    dq_type="INVALID_VALUE",
    dq_description="Payment Method ID should be greater than 0",
    check_category="PRE_JOIN",
    check_name="Primary Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=invalid_paymentmethodid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------


duplicate_paymentmethodid = (
    paymentmethod_df
    .groupBy("paymentmethodid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Payment Method IDs :", duplicate_paymentmethodid)

log_dq(
    table_name="application_paymentmethods",
    column_name="paymentmethodid",
    dq_type="DUPLICATE_CHECK",
    dq_description="Duplicate Payment Method IDs",
    check_category="PRE_JOIN",
    check_name="Duplicate Primary Key",
    severity="HIGH",
    total_records=total_records,
    failed_records=duplicate_paymentmethodid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_paymentmethodname = paymentmethod_df.filter(
    col("paymentmethodname").isNull() |
    (trim(col("paymentmethodname")) == "")
).count()

print("NULL Payment Method Name :", null_paymentmethodname)

log_dq(
    table_name="application_paymentmethods",
    column_name="paymentmethodname",
    dq_type="MANDATORY_FIELD",
    dq_description="Payment Method Name should not be NULL",
    check_category="PRE_JOIN",
    check_name="Mandatory Field Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_paymentmethodname,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------


null_validfrom = paymentmethod_df.filter(
    col("validfrom").isNull()
).count()

print("NULL Valid From :", null_validfrom)

log_dq(
    table_name="application_paymentmethods",
    column_name="validfrom",
    dq_type="MANDATORY_FIELD",
    dq_description="Valid From should not be NULL",
    check_category="PRE_JOIN",
    check_name="Valid From Check",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=null_validfrom,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------


null_validto = paymentmethod_df.filter(
    col("validto").isNull()
).count()

print("NULL Valid To :", null_validto)

log_dq(
    table_name="application_paymentmethods",
    column_name="validto",
    dq_type="MANDATORY_FIELD",
    dq_description="Valid To should not be NULL",
    check_category="PRE_JOIN",
    check_name="Valid To Check",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=null_validto,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

invalid_dates = paymentmethod_df.filter(
    col("validfrom") > col("validto")
).count()

print("Invalid Date Range :", invalid_dates)

log_dq(
    table_name="application_paymentmethods",
    column_name="validfrom, validto",
    dq_type="DATE_VALIDATION",
    dq_description="Valid From should not be greater than Valid To",
    check_category="PRE_JOIN",
    check_name="Date Range Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_dates,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------


null_lasteditedby = paymentmethod_df.filter(
    col("lasteditedby").isNull()
).count()

print("NULL Last Edited By :", null_lasteditedby)

log_dq(
    table_name="application_paymentmethods",
    column_name="lasteditedby",
    dq_type="MANDATORY_FIELD",
    dq_description="Last Edited By should not be NULL",
    check_category="PRE_JOIN",
    check_name="Last Edited By Validation",
    severity="LOW",
    total_records=total_records,
    failed_records=null_lasteditedby,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

spark.table(dq_logger_table) \
    .filter(
        (col("pipeline_run_id") == pipeline_run_id) &
        (col("check_category") == "PRE_JOIN")
    ) \
    .select(
        "table_name",
        "column_name",
        "dq_type",
        "failed_records",
        "dq_pass_rate"
    ) \
    .show(truncate=False)

# COMMAND ----------


spark.table(dq_logger_table) \
    .filter(col("pipeline_run_id") == pipeline_run_id) \
    .select(
        "table_name",
        "column_name",
        "check_name",
        "severity",
        "total_records",
        "failed_records",
        "issue_percentage",
        "dq_pass_rate"
    ) \
    .show(truncate=False)

# COMMAND ----------


dq_count = (
    spark.table(dq_logger_table)
    .filter(col("pipeline_run_id") == pipeline_run_id)
    .count()
)
print("Total DQ Checks Logged :", dq_count)

# COMMAND ----------

# ==========================================
# Task 9 : Filter Valid Payment Method Records
# ==========================================

from pyspark.sql.functions import col, trim

paymentmethod_valid_df = (
    paymentmethod_df
    .filter(col("paymentmethodid").isNotNull())
    .filter(col("paymentmethodid") > 0)
    .filter(col("paymentmethodname").isNotNull())
    .filter(trim(col("paymentmethodname")) != "")
    .filter(col("validfrom").isNotNull())
    .filter(col("validto").isNotNull())
    .filter(col("validfrom") <= col("validto"))
    .filter(col("lasteditedby").isNotNull())
)

print("Original Records :", paymentmethod_df.count())
print("Valid Records    :", paymentmethod_valid_df.count())
print("Rejected Records :", paymentmethod_df.count() - paymentmethod_valid_df.count())

# COMMAND ----------

paymentmethod_valid_df.select(
    "paymentmethodid",
    "paymentmethodname",
    "validfrom",
    "validto",
    "lasteditedby",
    "business_month",
    "ingested_timestamp"
).show(10, truncate=False)

# COMMAND ----------


post_total_records = paymentmethod_valid_df.count()
print("Post Validation Record Count :", post_total_records)

# COMMAND ----------

post_null_name = paymentmethod_valid_df.filter(
    col("paymentmethodname").isNull() |
    (trim(col("paymentmethodname")) == "")
).count()

print("Post Validation - NULL Payment Method Name :", post_null_name)

log_dq(
    table_name="application_paymentmethods",
    column_name="paymentmethodname",
    dq_type="POST_VALIDATION",
    dq_description="Payment Method Name should not be NULL after filtering",
    check_category="POST_JOIN",
    check_name="Post Validation - Payment Method Name",
    severity="HIGH",
    total_records=post_total_records,
    failed_records=post_null_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

post_invalid_dates = paymentmethod_valid_df.filter(
    col("validfrom") > col("validto")
).count()

print("Post Validation - Invalid Date Range :", post_invalid_dates)

log_dq(
    table_name="application_paymentmethods",
    column_name="validfrom, validto",
    dq_type="POST_VALIDATION",
    dq_description="Valid From should not be greater than Valid To after filtering",
    check_category="POST_JOIN",
    check_name="Post Validation - Date Range",
    severity="MEDIUM",
    total_records=post_total_records,
    failed_records=post_invalid_dates,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------


display(
    spark.table(dq_logger_table)
    .filter(
        (col("pipeline_run_id") == pipeline_run_id) &
        (col("check_category") == "POST_JOIN")
    )
)

# COMMAND ----------

# ==========================================
# Task 13 : Filter Final Records
# ==========================================

from pyspark.sql.functions import col, trim

paymentmethod_final_df = (
    paymentmethod_valid_df
    .filter(col("paymentmethodid").isNotNull())
    .filter(col("paymentmethodname").isNotNull())
    .filter(trim(col("paymentmethodname")) != "")
    .filter(col("validfrom").isNotNull())
    .filter(col("validto").isNotNull())
    .filter(col("lasteditedby").isNotNull())
)

print("Records Before Filtering :", paymentmethod_valid_df.count())
print("Valid Silver Records     :", paymentmethod_final_df.count())
print("Rejected Records         :", paymentmethod_valid_df.count() - paymentmethod_final_df.count())

# COMMAND ----------


paymentmethod_final_df.select(
    "paymentmethodid",
    "paymentmethodname",
    "validfrom",
    "validto",
    "lasteditedby",
    "business_month",
    "ingested_timestamp"
).show(10, truncate=False)

# COMMAND ----------

# ==========================================
# Task 14 : Align Final Dataset with Silver Schema
# ==========================================

from pyspark.sql.functions import col, current_timestamp, lit

paymentmethod_silver_df = (
    paymentmethod_final_df
    .select(
        col("paymentmethodid").alias("wwi_payment_method_id"),
        col("paymentmethodname").alias("payment_method"),
        col("validfrom").alias("valid_from"),
        col("validto").alias("valid_to"),
        col("lasteditedby").alias("last_edited_by"),
        col("business_month"),
        col("ingested_timestamp")
    )
    .withColumn("created_timestamp", current_timestamp())
    .withColumn("last_updated_timestamp", current_timestamp())
    .withColumn("pipeline_run_id", lit(pipeline_run_id))
)

# COMMAND ----------

paymentmethod_silver_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 15 : Delta MERGE (SCD Type 1)
# ==========================================

from delta.tables import DeltaTable

# Get Silver Table Reference
silver_delta_table = DeltaTable.forName(
    spark,
    silver_paymentmethod_table
)

# MERGE (SCD Type 1)
(
    silver_delta_table.alias("target")
    .merge(
        paymentmethod_silver_df.alias("source"),
        """
        target.wwi_payment_method_id = source.wwi_payment_method_id
        AND target.business_month = source.business_month
        """
    )

    # Update existing records
    .whenMatchedUpdate(
        set={
            "payment_method": "source.payment_method",
            "valid_from": "source.valid_from",
            "valid_to": "source.valid_to",
            "last_edited_by": "source.last_edited_by",
            "business_month": "source.business_month",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    # Insert new records
    .whenNotMatchedInsert(
        values={
            "wwi_payment_method_id": "source.wwi_payment_method_id",
            "payment_method": "source.payment_method",
            "valid_from": "source.valid_from",
            "valid_to": "source.valid_to",
            "last_edited_by": "source.last_edited_by",
            "business_month": "source.business_month",
            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    .execute()
)

# COMMAND ----------

display(
    spark.table(silver_paymentmethod_table)
)

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

silver_record_count = spark.table(silver_paymentmethod_table).count()

print("=" * 60)
print("Payment Method Staging Pipeline Completed Successfully")
print("=" * 60)

print(f"Business Month        : {business_month}")
print(f"Pipeline Run ID       : {pipeline_run_id}")
print(f"Total Silver Records  : {silver_record_count}")

print("=" * 60)
print(f"Data successfully loaded into {silver_paymentmethod_table}")
print("=" * 60)

# COMMAND ----------

display(
    spark.table(silver_paymentmethod_table)
)

# COMMAND ----------

