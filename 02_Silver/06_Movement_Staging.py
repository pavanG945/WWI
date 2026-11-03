# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_Common_Functions

# COMMAND ----------

# Task 1 : Business Month Widget

dbutils.widgets.text(
    "business_month",
    "2016-06",
    "Business Month"
)

business_month = dbutils.widgets.get("business_month")

if not business_month:
    raise Exception("Please select Business Month.")

print("Business Month :", business_month)

# COMMAND ----------

# import and Notebook Setup
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

import uuid

# ==========================================
# Task 2 : Notebook Setup
# ==========================================

# Bronze Table
bronze_movement_table = "wwi.bronze.warehouse_stockitemtransactions"

# Silver Table
silver_movement_table = "wwi.silver.movement_staging"

# DQ Logger Table
dq_logger_table = "wwi.logger.dq_logger"

# Generate Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("Pipeline Run ID :", pipeline_run_id)

# COMMAND ----------

# Task 3 : Verify DQ Logger Table

if spark.catalog.tableExists(dq_logger_table):
    print(f"DQ Logger table exists : {dq_logger_table}")
else:
    raise Exception(f"DQ Logger table not found : {dq_logger_table}")

# COMMAND ----------

spark.table(dq_logger_table).printSchema()

# COMMAND ----------

# ==========================================
# Task 4 : Verify Common DQ Function
# ==========================================

print(log_dq)

# COMMAND ----------

# ==========================================
# Task 5 : Load Bronze Movement Table
# ==========================================

movement_df = (
    spark.table(bronze_movement_table)
    .filter(col("business_month") == business_month)
)

# COMMAND ----------


print("=" * 60)

print("Movement Records :", movement_df.count())

print("=" * 60)

if movement_df.count() == 0:
    raise Exception(
        "No Movement records found for selected Business Month."
    )

# COMMAND ----------

movement_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 6 : Standardize Movement Data
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

movement_df = (
    movement_df

    # =====================================
    # Cast ID Columns
    # =====================================
    .withColumn("stockitemtransactionid", col("stockitemtransactionid").cast("double").cast("int"))
    .withColumn("stockitemid", col("stockitemid").cast("double").cast("int"))
    .withColumn("customerid", col("customerid").cast("double").cast("int"))
    .withColumn("supplierid", col("supplierid").cast("double").cast("int"))
    .withColumn("invoiceid", col("invoiceid").cast("double").cast("int"))
    .withColumn("purchaseorderid", col("purchaseorderid").cast("double").cast("int"))
    .withColumn("transactiontypeid", col("transactiontypeid").cast("double").cast("int"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    # =====================================
    # Cast Quantity
    # =====================================
    .withColumn("quantity", col("quantity").cast("double"))

    # =====================================
    # Convert Date Columns
    # =====================================
    .withColumn("transactionoccurredwhen", to_timestamp(col("transactionoccurredwhen"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("lasteditedwhen", to_timestamp(col("lasteditedwhen"), "dd-MM-yyyy HH:mm:ss"))

    # =====================================
    # Trim String Columns
    # =====================================
    .withColumn("business_month", trim(col("business_month")))
)

# COMMAND ----------

movement_df.printSchema()

# COMMAND ----------


movement_df.select(
    "stockitemtransactionid",
    "stockitemid",
    "transactiontypeid",
    "customerid",
    "invoiceid",
    "supplierid",
    "purchaseorderid",
    "quantity",
    "lasteditedwhen",
    "transactionoccurredwhen",
    "business_month"
).show(10, truncate=False)

# COMMAND ----------

movement_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 7 : Pre-Join Data Quality Checks
# ==========================================

total_records = movement_df.count()

print("Total Movement Records :", total_records)

# COMMAND ----------

null_transaction_id = movement_df.filter(
    col("stockitemtransactionid").isNull()
).count()

print("NULL Stock Item Transaction ID :", null_transaction_id)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="stockitemtransactionid",
    dq_type="NULL_CHECK",
    dq_description="Stock Item Transaction ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Primary Key NULL Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_transaction_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

invalid_transaction_id = movement_df.filter(
    col("stockitemtransactionid") <= 0
).count()

print("Invalid Stock Item Transaction ID :", invalid_transaction_id)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="stockitemtransactionid",
    dq_type="INVALID_VALUE",
    dq_description="Stock Item Transaction ID should be greater than 0",
    check_category="PRE_JOIN",
    check_name="Primary Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=invalid_transaction_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

duplicate_transaction_id = (
    movement_df
    .groupBy("stockitemtransactionid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Transaction IDs :", duplicate_transaction_id)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="stockitemtransactionid",
    dq_type="DUPLICATE_CHECK",
    dq_description="Duplicate Stock Item Transaction IDs",
    check_category="PRE_JOIN",
    check_name="Duplicate Primary Key",
    severity="HIGH",
    total_records=total_records,
    failed_records=duplicate_transaction_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_stockitemid = movement_df.filter(
    col("stockitemid").isNull()
).count()

print("NULL Stock Item ID :", null_stockitemid)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="stockitemid",
    dq_type="MANDATORY_FIELD",
    dq_description="Stock Item ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Stock Item Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_stockitemid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_transactiontypeid = movement_df.filter(
    col("transactiontypeid").isNull()
).count()

print("NULL Transaction Type ID :", null_transactiontypeid)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="transactiontypeid",
    dq_type="MANDATORY_FIELD",
    dq_description="Transaction Type ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Transaction Type Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_transactiontypeid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Transaction Type Lookup Check
null_transactiontype = movement_df.filter(
    col("transactiontypeid").isNull()
).count()

print("NULL Transaction Type ID :", null_transactiontype)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="transactiontypeid",
    dq_type="LOOKUP_KEY",
    dq_description="Transaction Type ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Lookup Key Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=null_transactiontype,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

invalid_quantity = movement_df.filter(
    (col("quantity").isNull()) |
    (col("quantity") == 0)
).count()

print("Invalid Quantity :", invalid_quantity)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="quantity",
    dq_type="INVALID_VALUE",
    dq_description="Quantity should not be NULL or Zero",
    check_category="PRE_JOIN",
    check_name="Quantity Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_quantity,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_lasteditedwhen = movement_df.filter(
    col("lasteditedwhen").isNull()
).count()

print("NULL Last Edited When :", null_lasteditedwhen)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="lasteditedwhen",
    dq_type="MANDATORY_FIELD",
    dq_description="Last Edited When should not be NULL",
    check_category="PRE_JOIN",
    check_name="Last Edited Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=null_lasteditedwhen,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

display(
    spark.table(dq_logger_table)
    .filter(col("pipeline_run_id") == pipeline_run_id)
)

# COMMAND ----------

# ==========================================
# Task 8 : Verify DQ Logs
# ==========================================

display(
    spark.table(dq_logger_table)
    .filter(col("pipeline_run_id") == pipeline_run_id)
    .orderBy("check_timestamp")
)

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


dq_check_count = (
    spark.table(dq_logger_table)
    .filter(col("pipeline_run_id") == pipeline_run_id)
    .count()
)

print("=" * 60)
print("Total DQ Checks Logged :", dq_check_count)
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 9 : Filter Valid Movement Records
# ==========================================

from pyspark.sql.functions import col

movement_valid_df = (
    movement_df
    .filter(col("stockitemtransactionid").isNotNull())
    .filter(col("stockitemtransactionid") > 0)
    .filter(col("stockitemid").isNotNull())
    .filter(col("transactiontypeid").isNotNull())
    .filter(col("quantity").isNotNull())
    .filter(col("quantity") != 0)
    .filter(col("lasteditedwhen").isNotNull())
)

print("Records Before Filtering :", movement_df.count())
print("Valid Records            :", movement_valid_df.count())
print("Rejected Records         :", movement_df.count() - movement_valid_df.count())

# COMMAND ----------

movement_valid_df.printSchema()

# COMMAND ----------

movement_valid_df.select(
    "stockitemtransactionid",
    "stockitemid",
    "transactiontypeid",
    "customerid",
    "invoiceid",
    "supplierid",
    "purchaseorderid",
    "quantity",
    "lasteditedwhen",
    "business_month"
).show(10, truncate=False)

# COMMAND ----------

# ==========================================
# Task 10 : Prepare Lookup Table
# ==========================================

transactiontype_lookup_df = (
    spark.table("wwi.bronze.application_transactiontypes")
    .filter(col("business_month") == business_month)
    .select(
        "transactiontypeid",
        "transactiontypename"
    )
    .dropDuplicates(["transactiontypeid"])
)

print("Transaction Type Lookup Records :", transactiontype_lookup_df.count())

# COMMAND ----------

# ==========================================
# Task 10 : Prepare Stock Item Lookup
# ==========================================

stockitem_lookup_df = (
    movement_df
    .dropDuplicates(["stockitemid"])
)

print("Original Stock Item Records :", movement_df.count())
print("Unique Stock Item Records   :", stockitem_lookup_df.count())

# COMMAND ----------

transactiontype_lookup_df.printSchema()

# COMMAND ----------

duplicate_keys = (
    transactiontype_lookup_df
    .groupBy("transactiontypeid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Transaction Type IDs :", duplicate_keys)

# COMMAND ----------

# ==========================================
# Task 12 : Post-Validation Checks
# ==========================================

post_total_records = movement_valid_df.count()

print("=" * 60)
print("Post Validation Record Count :", post_total_records)
print("=" * 60)

# COMMAND ----------

post_null_transaction_id = movement_valid_df.filter(
    col("stockitemtransactionid").isNull()
).count()

print("Post Validation - NULL Transaction ID :", post_null_transaction_id)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="stockitemtransactionid",
    dq_type="POST_VALIDATION",
    dq_description="Stock Item Transaction ID should not be NULL after filtering",
    check_category="POST_JOIN",
    check_name="Post Validation - Transaction ID",
    severity="HIGH",
    total_records=post_total_records,
    failed_records=post_null_transaction_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

post_invalid_quantity = movement_valid_df.filter(
    (col("quantity").isNull()) |
    (col("quantity") == 0)
).count()

print("Post Validation - Invalid Quantity :", post_invalid_quantity)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="quantity",
    dq_type="POST_VALIDATION",
    dq_description="Quantity should not be NULL or Zero after filtering",
    check_category="POST_JOIN",
    check_name="Post Validation - Quantity",
    severity="MEDIUM",
    total_records=post_total_records,
    failed_records=post_invalid_quantity,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

post_null_lasteditedwhen = movement_valid_df.filter(
    col("lasteditedwhen").isNull()
).count()

print("Post Validation - NULL Last Edited When :", post_null_lasteditedwhen)

log_dq(
    table_name="warehouse_stockitemtransactions",
    column_name="lasteditedwhen",
    dq_type="POST_VALIDATION",
    dq_description="Last Edited When should not be NULL after filtering",
    check_category="POST_JOIN",
    check_name="Post Validation - Last Edited When",
    severity="MEDIUM",
    total_records=post_total_records,
    failed_records=post_null_lasteditedwhen,
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

from pyspark.sql.functions import col

movement_final_df = (
    movement_valid_df
    .filter(col("stockitemtransactionid").isNotNull())
    .filter(col("stockitemid").isNotNull())
    .filter(col("transactiontypeid").isNotNull())
    .filter(col("quantity").isNotNull())
    .filter(col("lasteditedwhen").isNotNull())
)

print("Records Before Filtering :", movement_valid_df.count())
print("Valid Silver Records     :", movement_final_df.count())
print("Rejected Records         :", movement_valid_df.count() - movement_final_df.count())

# COMMAND ----------

movement_final_df.printSchema()

# COMMAND ----------

movement_final_df.select(
    "stockitemtransactionid",
    "stockitemid",
    "transactiontypeid",
    "customerid",
    "invoiceid",
    "supplierid",
    "purchaseorderid",
    "quantity",
    "lasteditedwhen",
    "business_month"
).show(10, truncate=False)

# COMMAND ----------

# ==========================================
# Task 14 : Align Final Dataset with Silver Schema
# ==========================================

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    date_format
)

movement_silver_df = (
    movement_final_df

    .select(

        col("stockitemtransactionid").alias("wwi_stock_item_transaction_id"),

        col("stockitemid").alias("wwi_stock_item_id"),

        col("transactiontypeid").alias("wwi_transaction_type_id"),

        col("customerid").alias("wwi_customer_id"),

        col("invoiceid").alias("wwi_invoice_id"),

        col("supplierid").alias("wwi_supplier_id"),

        col("purchaseorderid").alias("wwi_purchase_order_id"),

        date_format(
            col("transactionoccurredwhen"),
            "yyyyMMdd"
        ).cast("int").alias("date_key"),

        col("quantity"),

        col("business_month"),

        col("lasteditedwhen").alias("last_modified_when")
    )

    .withColumn(
        "created_timestamp",
        current_timestamp()
    )

    .withColumn(
        "last_updated_timestamp",
        current_timestamp()
    )

    .withColumn(
        "pipeline_run_id",
        lit(pipeline_run_id)
    )
)

# COMMAND ----------

movement_silver_df.printSchema()

# COMMAND ----------

display(movement_silver_df)

# COMMAND ----------

print("Movement Silver Records :", movement_silver_df.count())

# COMMAND ----------

# ==========================================
# Task 15 : Delta MERGE (SCD Type 1)
# ==========================================

from delta.tables import DeltaTable

# Get Silver Table Reference
silver_delta_table = DeltaTable.forName(
    spark,
    silver_movement_table
)

# MERGE (SCD Type 1)
(
    silver_delta_table.alias("target")
    .merge(
        movement_silver_df.alias("source"),
        """
        target.wwi_stock_item_transaction_id =
        source.wwi_stock_item_transaction_id
        """
    )

    # Update Existing Records
    .whenMatchedUpdate(
        set={
            "wwi_stock_item_id": "source.wwi_stock_item_id",
            "wwi_transaction_type_id": "source.wwi_transaction_type_id",
            "wwi_customer_id": "source.wwi_customer_id",
            "wwi_invoice_id": "source.wwi_invoice_id",
            "wwi_supplier_id": "source.wwi_supplier_id",
            "wwi_purchase_order_id": "source.wwi_purchase_order_id",
            "date_key": "source.date_key",
            "quantity": "source.quantity",
            "business_month": "source.business_month",
            "last_modified_when": "source.last_modified_when",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    # Insert New Records
    .whenNotMatchedInsert(
        values={
            "wwi_stock_item_transaction_id": "source.wwi_stock_item_transaction_id",
            "wwi_stock_item_id": "source.wwi_stock_item_id",
            "wwi_transaction_type_id": "source.wwi_transaction_type_id",
            "wwi_customer_id": "source.wwi_customer_id",
            "wwi_invoice_id": "source.wwi_invoice_id",
            "wwi_supplier_id": "source.wwi_supplier_id",
            "wwi_purchase_order_id": "source.wwi_purchase_order_id",
            "date_key": "source.date_key",
            "quantity": "source.quantity",
            "business_month": "source.business_month",
            "last_modified_when": "source.last_modified_when",
            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    .execute()
)

# COMMAND ----------

display(
    spark.table(silver_movement_table)
)

# COMMAND ----------

silver_record_count = spark.table(
    silver_movement_table
).count()

print("Movement Silver Records :", silver_record_count)

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

silver_record_count = spark.table(silver_movement_table).count()

print("=" * 60)
print("Movement Staging Pipeline Completed Successfully")
print("=" * 60)

print(f"Business Month        : {business_month}")
print(f"Pipeline Run ID       : {pipeline_run_id}")
print(f"Total Silver Records  : {silver_record_count}")

print("=" * 60)
print(f"Data successfully loaded into {silver_movement_table}")
print("=" * 60)

# COMMAND ----------

display(
    spark.table(silver_movement_table).limit(10)
)

# COMMAND ----------

display(spark.table(silver_movement_table).limit(10))

# COMMAND ----------

