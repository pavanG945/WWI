# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_Common_Functions

# COMMAND ----------

# ==========================================
# Task 1 : Business Month Widget
# ==========================================

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

import uuid

# ==========================================
# Bronze Tables
# ==========================================

bronze_employee_table = "wwi.bronze.application_people"

# Lookup Tables
bronze_department_table = "wwi.bronze.application_systemparameters"

# Silver Table
silver_employee_table = "wwi.silver.employee_staging"

# DQ Logger
dq_logger_table = "wwi.logger.dq_logger"

# Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("Pipeline Run ID :", pipeline_run_id)

# COMMAND ----------

spark.catalog.tableExists(dq_logger_table)
spark.table(dq_logger_table).printSchema()

# COMMAND ----------

print(log_dq)

# COMMAND ----------

from pyspark.sql.functions import col

# ==========================================
# Task 5 : Load Bronze Employee Table
# ==========================================

employee_df = (
    spark.table(bronze_employee_table)
    .filter(col("business_month") == business_month)
)

print("Employee Records :", employee_df.count())

employee_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# ==========================================
# Task 6 : Standardize Employee Data
# ==========================================

employee_df = (

    employee_df

    # ======================================
    # Cast ID Columns
    # ======================================
    .withColumn(
        "personid",
        col("personid").cast("double").cast("int")
    )

    .withColumn(
        "lasteditedby",
        col("lasteditedby").cast("double").cast("int")
    )

    # ======================================
    # Trim Text Columns
    # ======================================
    .withColumn("fullname", trim(col("fullname")))
    .withColumn("preferredname", trim(col("preferredname")))
    .withColumn("searchname", trim(col("searchname")))
    .withColumn("emailaddress", trim(lower(col("emailaddress"))))
    .withColumn("phonenumber", trim(col("phonenumber")))
    .withColumn("faxnumber", trim(col("faxnumber")))
    .withColumn("logonname", trim(col("logonname")))
    .withColumn("otherlanguages", trim(col("otherlanguages")))

    # ======================================
    # Title Case
    # ======================================
    .withColumn(
        "fullname",
        initcap(col("fullname"))
    )

    .withColumn(
        "preferredname",
        initcap(col("preferredname"))
    )

    # ======================================
    # Email Validation
    # ======================================
    .withColumn(
        "is_valid_email",
        col("emailaddress").rlike(
            "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
        )
    )

    # ======================================
    # Phone Validation
    # ======================================
    .withColumn(
        "is_valid_phone",
        col("phonenumber").rlike(
            "^[0-9 +()-]{6,20}$"
        )
    )

)

# COMMAND ----------

employee_df.printSchema()

# COMMAND ----------

display(
    employee_df.select(
        "personid",
        "fullname",
        "preferredname",
        "emailaddress",
        "phonenumber",
        "isemployee",
        "issalesperson",
        "is_valid_email",
        "is_valid_phone"
    )
)

# COMMAND ----------

employee_df.select(
    "personid",
    "lasteditedby"
).printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, trim

# ==========================================
# Task 7.1 : Total Records
# ==========================================

total_records = employee_df.count()

print("Total Employee Records :", total_records)

# COMMAND ----------

# ==========================================
# Primary Key NULL Check
# ==========================================

null_personid = employee_df.filter(
    col("personid").isNull()
).count()

print("NULL Person ID :", null_personid)

log_dq(
    table_name="application_people",
    column_name="personid",
    dq_type="NULL_CHECK",
    dq_description="Person ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Primary Key NULL Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_personid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Invalid Person ID Check
# ==========================================

invalid_personid = employee_df.filter(
    col("personid") <= 0
).count()

print("Invalid Person ID :", invalid_personid)

log_dq(
    table_name="application_people",
    column_name="personid",
    dq_type="INVALID_VALUE",
    dq_description="Person ID should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Primary Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=invalid_personid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Duplicate Person ID Check
# ==========================================

duplicate_personid = (
    employee_df
    .groupBy("personid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Person IDs :", duplicate_personid)

log_dq(
    table_name="application_people",
    column_name="personid",
    dq_type="DUPLICATE_CHECK",
    dq_description="Person ID should be unique",
    check_category="PRE_JOIN",
    check_name="Duplicate Primary Key Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=duplicate_personid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Mandatory Full Name Check
# ==========================================

missing_fullname = employee_df.filter(
    col("fullname").isNull() |
    (trim(col("fullname")) == "")
).count()

print("Missing Full Name :", missing_fullname)

log_dq(
    table_name="application_people",
    column_name="fullname",
    dq_type="MANDATORY_FIELD",
    dq_description="Full Name should not be NULL or blank",
    check_category="PRE_JOIN",
    check_name="Mandatory Field Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_fullname,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Mandatory Preferred Name Check
# ==========================================

missing_preferredname = employee_df.filter(
    col("preferredname").isNull() |
    (trim(col("preferredname")) == "")
).count()

print("Missing Preferred Name :", missing_preferredname)

log_dq(
    table_name="application_people",
    column_name="preferredname",
    dq_type="MANDATORY_FIELD",
    dq_description="Preferred Name should not be NULL or blank",
    check_category="PRE_JOIN",
    check_name="Mandatory Field Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_preferredname,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Employee Flag Validation
# ==========================================

non_employee_records = employee_df.filter(
    lower(col("isemployee")) != "true"
).count()

print("Non Employee Records :", non_employee_records)

log_dq(
    table_name="application_people",
    column_name="isemployee",
    dq_type="BUSINESS_RULE",
    dq_description="Only employee records should be processed",
    check_category="PRE_JOIN",
    check_name="Employee Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=non_employee_records,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

from pyspark.sql.functions import col

# ==========================================
# Task 8 : Verify Pre-Join DQ Logs
# ==========================================

spark.table(dq_logger_table) \
    .filter(col("pipeline_run_id") == pipeline_run_id) \
    .select(
        "table_name",
        "column_name",
        "dq_type",
        "severity",
        "total_records",
        "failed_records",
        "issue_percentage",
        "dq_pass_rate"
    ) \
    .show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower

# ==========================================
# Task 9 : Filter Valid Employee Records
# ==========================================

employee_valid_df = (
    employee_df

    # Primary Key Validation
    .filter(col("personid").isNotNull())
    .filter(col("personid") > 0)

    # Mandatory Fields
    .filter(col("fullname").isNotNull())
    .filter(trim(col("fullname")) != "")

    .filter(col("preferredname").isNotNull())
    .filter(trim(col("preferredname")) != "")

    # Business Rule
    .filter(lower(col("isemployee")) == "true")
)

# COMMAND ----------

print("Original Records :", employee_df.count())
print("Valid Records    :", employee_valid_df.count())
print("Rejected Records :", employee_df.count() - employee_valid_df.count())

# COMMAND ----------

# ==========================================
# Task 13 : Create Final Employee DataFrame
# ==========================================

employee_final_df = employee_valid_df

print("Final Employee Records :", employee_final_df.count())

# COMMAND ----------

# ==========================================
# Verify Final Employee DataFrame
# ==========================================

display(
    employee_final_df.select(
        "personid",
        "fullname",
        "preferredname",
        "emailaddress",
        "isemployee",
        "issalesperson"
    )
)

# COMMAND ----------

spark.table(silver_employee_table).printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

# ==========================================
# Task 14 : Align Dataset With Silver Schema
# ==========================================

employee_silver_df = (
    employee_final_df

    # Select and rename business columns
    .select(
        col("personid").cast("int").alias("wwi_employee_id"),
        col("fullname").alias("employee"),
        col("preferredname").alias("preferred_name"),
        col("issalesperson").cast("boolean").alias("is_salesperson"),
        col("photo")
    )

    # Add metadata columns
    .withColumn(
        "business_month",
        lit(business_month)
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

print("Final Silver Records :", employee_silver_df.count())

# COMMAND ----------

# ==========================================
# Verify Task 14 Output Schema
# ==========================================

employee_silver_df.printSchema()

# COMMAND ----------

from delta.tables import DeltaTable

# ==========================================
# Task 15 : Delta MERGE into Silver Employee Table
# ==========================================

# Get Silver Delta Table Reference
silver_delta_table = DeltaTable.forName(
    spark,
    silver_employee_table
)

# Perform SCD Type 1 MERGE
(
    silver_delta_table.alias("target")
    .merge(
        employee_silver_df.alias("source"),
        """
        target.wwi_employee_id = source.wwi_employee_id
        AND target.business_month = source.business_month
        """
    )

    # Update existing employee records
    .whenMatchedUpdate(
        set={
            "employee": "source.employee",
            "preferred_name": "source.preferred_name",
            "is_salesperson": "source.is_salesperson",
            "photo": "source.photo",
            "business_month": "source.business_month",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    # Insert new employee records
    .whenNotMatchedInsert(
        values={
            "wwi_employee_id": "source.wwi_employee_id",
            "employee": "source.employee",
            "preferred_name": "source.preferred_name",
            "is_salesperson": "source.is_salesperson",
            "photo": "source.photo",
            "business_month": "source.business_month",
            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    .execute()
)

print("Employee Silver MERGE completed successfully")

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

silver_record_count = spark.table(silver_employee_table).count()

print("=" * 60)
print("Employee Staging Pipeline Completed Successfully")
print("=" * 60)

print(f"Business Month        : {business_month}")
print(f"Pipeline Run ID       : {pipeline_run_id}")
print(f"Total Silver Records  : {silver_record_count}")

print("=" * 60)
print(f"Data successfully loaded into: {silver_employee_table}")
print("=" * 60)

# COMMAND ----------

display(
    spark.table(silver_employee_table)
)

# COMMAND ----------

