# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_Common_Functions

# COMMAND ----------

# ==========================================
# Task 1 : Create Business Month Widget
# ==========================================

dbutils.widgets.text(
    "business_month",
    "2016-06",
    "Business Month"
)

business_month = dbutils.widgets.get("business_month").strip()

if not business_month:
    raise Exception(
        "Business Month is required. Please enter a value (e.g., 2016-06)."
    )

print(f"Business Month : {business_month}")

# COMMAND ----------

# ==========================================
# Task 2 : Notebook Setup
# ==========================================

import uuid

# Bronze Source Table
bronze_transaction_type_table = "wwi.bronze.application_transactiontypes"

# Silver Target Table
silver_transaction_type_table = "wwi.silver.transactiontype_staging"

# Centralized DQ Logger Table
dq_logger_table = "wwi.logger.dq_logger"

# Unique Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("=" * 60)
print("Notebook Setup Completed")
print("=" * 60)

print(f"Bronze Table      : {bronze_transaction_type_table}")
print(f"Silver Table      : {silver_transaction_type_table}")
print(f"DQ Logger Table   : {dq_logger_table}")
print(f"Pipeline Run ID   : {pipeline_run_id}")

# COMMAND ----------

# ==========================================
# Task 3 : Verify DQ Logger Table
# ==========================================

try:

    spark.table(dq_logger_table)

    print("=" * 60)
    print("DQ Logger Table Verification")
    print("=" * 60)
    print(f"{dq_logger_table} is available.")

except Exception as e:

    raise Exception(
        f"DQ Logger table '{dq_logger_table}' does not exist or is not accessible.\n{e}"
    )

# COMMAND ----------

print(log_dq)

# COMMAND ----------

# ==========================================
# Task 5 : Load Bronze Table
# ==========================================

transaction_type_df = (

    spark.table(bronze_transaction_type_table)

    .filter(
        col("business_month") == business_month
    )

)

print("=" * 60)
print("Bronze Table Loaded Successfully")
print("=" * 60)

print("Business Month :", business_month)
print("Total Records  :", transaction_type_df.count())

transaction_type_df.printSchema()

display(transaction_type_df)

# COMMAND ----------

# ==========================================
# Task 6 : Standardize Transaction Type Table
# ==========================================

from pyspark.sql.functions import col, trim, initcap, to_timestamp

transaction_type_df = (

    transaction_type_df

    # IDs
    .withColumn(
        "transactiontypeid",
        col("transactiontypeid").cast("double").cast("integer")
    )

    .withColumn(
        "lasteditedby",
        col("lasteditedby").cast("double").cast("integer")
    )

    # Text
    .withColumn(
        "transactiontypename",
        initcap(trim(col("transactiontypename")))
    )

    # Dates
    .withColumn(
        "validfrom",
        to_timestamp(col("validfrom"), "dd-MM-yyyy HH:mm:ss")
    )

    .withColumn(
        "validto",
        to_timestamp(col("validto"), "dd-MM-yyyy HH:mm:ss")
    )

)

print("=" * 60)
print("Transaction Type Table Standardized")
print("=" * 60)

transaction_type_df.printSchema()

display(transaction_type_df)

# COMMAND ----------

# ==========================================
# Task 7 : Pre-Join Data Quality Checks
# ==========================================

total_records = transaction_type_df.count()

# Primary Key Checks
missing_transaction_type_id = transaction_type_df.filter(
    col("transactiontypeid").isNull()
).count()

duplicate_transaction_type_id = (

    transaction_type_df

    .groupBy("transactiontypeid")
    .count()

    .filter(
        (col("transactiontypeid").isNotNull()) &
        (col("count") > 1)
    )

    .count()

)

# Required Field
missing_transaction_type_name = transaction_type_df.filter(
    col("transactiontypename").isNull()
).count()

# Valid From
missing_valid_from = transaction_type_df.filter(
    col("validfrom").isNull()
).count()

# Valid To
missing_valid_to = transaction_type_df.filter(
    col("validto").isNull()
).count()

print("=" * 60)
print("Transaction Type DQ Summary")
print("=" * 60)

print("Total Records                 :", total_records)
print("Missing Transaction Type ID   :", missing_transaction_type_id)
print("Duplicate Transaction Type ID :", duplicate_transaction_type_id)
print("Missing Transaction Type Name :", missing_transaction_type_name)
print("Missing Valid From            :", missing_valid_from)
print("Missing Valid To              :", missing_valid_to)

# COMMAND ----------

# ==========================================
# Task 8 : Log Pre-Join DQ Results
# ==========================================

# Transaction Type ID
log_dq(
    table_name="transaction_type_staging",
    column_name="transactiontypeid",
    dq_type="NULL_CHECK",
    dq_description="Transaction Type ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Transaction Type ID Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_transaction_type_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Duplicate Transaction Type ID
log_dq(
    table_name="transaction_type_staging",
    column_name="transactiontypeid",
    dq_type="DUPLICATE_CHECK",
    dq_description="Transaction Type ID should be unique",
    check_category="PRE_JOIN",
    check_name="Duplicate Transaction Type ID Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=duplicate_transaction_type_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Transaction Type Name
log_dq(
    table_name="transaction_type_staging",
    column_name="transactiontypename",
    dq_type="NULL_CHECK",
    dq_description="Transaction Type Name should not be NULL",
    check_category="PRE_JOIN",
    check_name="Transaction Type Name Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_transaction_type_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Valid From
log_dq(
    table_name="transaction_type_staging",
    column_name="validfrom",
    dq_type="NULL_CHECK",
    dq_description="Valid From should not be NULL",
    check_category="PRE_JOIN",
    check_name="Valid From Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_valid_from,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Valid To
log_dq(
    table_name="transaction_type_staging",
    column_name="validto",
    dq_type="NULL_CHECK",
    dq_description="Valid To should not be NULL",
    check_category="PRE_JOIN",
    check_name="Valid To Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_valid_to,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)


# COMMAND ----------

# ==========================================
# Task 9 : Filter Valid Records
# ==========================================

transaction_type_valid_df = (

    transaction_type_df

    .filter(col("transactiontypeid").isNotNull())
    .filter(col("transactiontypename").isNotNull())
    .filter(col("validfrom").isNotNull())
    .filter(col("validto").isNotNull())

    .dropDuplicates(["transactiontypeid"])

)

print("=" * 60)
print("Transaction Type Filtering Summary")
print("=" * 60)

print("Original Records :", transaction_type_df.count())
print("Valid Records    :", transaction_type_valid_df.count())
print("Rejected Records :", transaction_type_df.count() - transaction_type_valid_df.count())

display(transaction_type_valid_df)

# COMMAND ----------

# ==========================================
# Task 10 : Prepare Lookup Table
# ==========================================

transaction_type_lookup_df = (

    transaction_type_valid_df

    .select(
        "transactiontypeid",
        "transactiontypename",
        "validfrom",
        "validto",
        "business_month",
        "lasteditedby"
    )

    .dropDuplicates(["transactiontypeid"])

)

print("=" * 60)
print("Transaction Type Lookup Prepared")
print("=" * 60)

print("Lookup Records :", transaction_type_lookup_df.count())

transaction_type_lookup_df.printSchema()

display(transaction_type_lookup_df)

# COMMAND ----------

# ==========================================
# Task 11 : Join Main Table with Lookup Tables
# ==========================================

transaction_type_join_df = (

    transaction_type_lookup_df

)

print("=" * 60)
print("No Lookup Join Required")
print("=" * 60)

print("Joined Records :", transaction_type_join_df.count())

transaction_type_join_df.printSchema()

display(transaction_type_join_df)

# COMMAND ----------

# ==========================================
# Task 12 : Post-Join DQ Checks
# ==========================================

total_records = transaction_type_join_df.count()

# Mandatory Fields
missing_transaction_type_id = transaction_type_join_df.filter(
    col("transactiontypeid").isNull()
).count()

missing_transaction_type_name = transaction_type_join_df.filter(
    col("transactiontypename").isNull()
).count()

missing_valid_from = transaction_type_join_df.filter(
    col("validfrom").isNull()
).count()

missing_valid_to = transaction_type_join_df.filter(
    col("validto").isNull()
).count()

print("=" * 60)
print("Post-Join DQ Summary")
print("=" * 60)

print("Total Records                 :", total_records)
print("Missing Transaction Type ID   :", missing_transaction_type_id)
print("Missing Transaction Type Name :", missing_transaction_type_name)
print("Missing Valid From            :", missing_valid_from)
print("Missing Valid To              :", missing_valid_to)

# COMMAND ----------

# Transaction Type ID
log_dq(
    table_name="transaction_type_staging",
    column_name="transactiontypeid",
    dq_type="POST_VALIDATION",
    dq_description="Transaction Type ID should not be NULL after processing",
    check_category="POST_JOIN",
    check_name="Transaction Type ID Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_transaction_type_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Transaction Type Name
log_dq(
    table_name="transaction_type_staging",
    column_name="transactiontypename",
    dq_type="POST_VALIDATION",
    dq_description="Transaction Type Name should not be NULL after processing",
    check_category="POST_JOIN",
    check_name="Transaction Type Name Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_transaction_type_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Valid From
log_dq(
    table_name="transaction_type_staging",
    column_name="validfrom",
    dq_type="POST_VALIDATION",
    dq_description="Valid From should not be NULL after processing",
    check_category="POST_JOIN",
    check_name="Valid From Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_valid_from,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Valid To
log_dq(
    table_name="transaction_type_staging",
    column_name="validto",
    dq_type="POST_VALIDATION",
    dq_description="Valid To should not be NULL after processing",
    check_category="POST_JOIN",
    check_name="Valid To Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_valid_to,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

print("=" * 60)
print("Task 12 Completed Successfully")
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 13 : Filter Final Records
# ==========================================

transaction_type_final_df = (

    transaction_type_join_df

    .filter(col("transactiontypeid").isNotNull())
    .filter(col("transactiontypename").isNotNull())
    .filter(col("validfrom").isNotNull())
    .filter(col("validto").isNotNull())

)

print("=" * 60)
print("Final Record Summary")
print("=" * 60)

print("Records Before :", transaction_type_join_df.count())
print("Records After  :", transaction_type_final_df.count())
print("Records Removed:", transaction_type_join_df.count() - transaction_type_final_df.count())

transaction_type_final_df.printSchema()

display(transaction_type_final_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col

transaction_type_silver_df = (
    transaction_type_final_df.select(
        col("transactiontypeid").alias("transaction_type_id"),
        col("transactiontypename").alias("transaction_type_name"),
        col("validfrom").alias("valid_from"),
        col("validto").alias("valid_to"),
        current_timestamp().alias("created_timestamp"),
        current_timestamp().alias("last_updated_timestamp"),
        lit(pipeline_run_id).alias("pipeline_run_id"),
        col("business_month")
    )
)

print("=" * 60)
print("Silver Dataset Prepared")
print("=" * 60)

transaction_type_silver_df.printSchema()

display(transaction_type_silver_df)

# COMMAND ----------

spark.table(silver_transaction_type_table).printSchema()

# COMMAND ----------

# ==========================================
# Task 15 : Delta MERGE (SCD Type 1)
# ==========================================

from delta.tables import DeltaTable

# Get Silver Delta Table
silver_delta_table = DeltaTable.forName(
    spark,
    silver_transaction_type_table
)

# Merge into Silver Table
(
    silver_delta_table.alias("target")
    .merge(
        transaction_type_silver_df.alias("source"),
        """
        target.transaction_type_id = source.transaction_type_id
        AND target.business_month = source.business_month
        """
    )

    # Update Existing Records
    .whenMatchedUpdate(
        set={
            "transaction_type_name": "source.transaction_type_name",
            "valid_from": "source.valid_from",
            "valid_to": "source.valid_to",
            "business_month": "source.business_month",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    # Insert New Records
    .whenNotMatchedInsert(
        values={
            "transaction_type_id": "source.transaction_type_id",
            "transaction_type_name": "source.transaction_type_name",
            "valid_from": "source.valid_from",
            "valid_to": "source.valid_to",
            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id",
            "business_month": "source.business_month"
        }
    )

    .execute()
)

print("=" * 60)
print("Task 15 : Delta MERGE Completed Successfully")
print("=" * 60)

print("Silver Transaction Type Records :",
      spark.table(silver_transaction_type_table).count())

display(spark.table(silver_transaction_type_table))

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

from pyspark.sql import Row

# Total records in Silver table
total_silver_records = spark.table(
    silver_transaction_type_table
).count()

print("=" * 60)
print("    TRANSACTION TYPE STAGING PIPELINE COMPLETED")
print("=" * 60)

print(f"Pipeline Run ID      : {pipeline_run_id}")
print(f"Business Month       : {business_month}")
print(f"Silver Table         : {silver_transaction_type_table}")
print(f"Total Silver Records : {total_silver_records}")

print("=" * 60)
print("Status               : SUCCESS")
print("Task                 : 13_TransactionType_Staging")
print("Delta MERGE          : Completed Successfully")
print("=" * 60)

# COMMAND ----------



# Create Summary DataFrame
summary_df = spark.createDataFrame([
    Row(
        pipeline_run_id=pipeline_run_id,
        business_month=business_month,
        silver_table=silver_transaction_type_table,
        total_records=total_silver_records,
        status="SUCCESS"
    )
])

display(summary_df)

# COMMAND ----------

