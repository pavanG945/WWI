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

# import and Notebook Setup
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

# MAGIC %md
# MAGIC Task 2: Set up notebook: define Bronze source, Silver target table, DQ logger table, generate unique pipeline_run_id

# COMMAND ----------

import uuid

# Bronze Tables
bronze_supplier_table = "wwi.bronze.purchasing_suppliers"
bronze_category_table = "wwi.bronze.purchasing_suppliercategories"
bronze_person_table = "wwi.bronze.application_people"

# Silver Table
silver_supplier_table = "wwi.silver.supplier_staging"

# DQ Logger
dq_logger_table = "wwi.logger.dq_logger"

# Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("Pipeline Run ID :", pipeline_run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 3 : Verify central DQ logger table is available in wwi.logger schema

# COMMAND ----------

spark.catalog.tableExists(dq_logger_table)
spark.table(dq_logger_table).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 4 : Create reusable DQ logging function: generate unique DQ ID per record, calculate failed count, issue percentage, pass rate, append to DQ logger

# COMMAND ----------

print(log_dq)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 5 : Load Bronze tables required for staging, filtered by business_month (June 2016)

# COMMAND ----------

supplier_df = spark.table(bronze_supplier_table).filter(col("business_month") == business_month)

supplier_category_df = spark.table(bronze_category_table).filter(col("business_month") == business_month)

person_df = spark.table(bronze_person_table).filter(col("business_month") == business_month)

supplier_df.printSchema()
supplier_category_df.printSchema()
person_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 6 : Standardize raw Bronze data: cast ID columns to integer, validate email/phone with regex, trim text fields, apply title case to names, clean postal codes

# COMMAND ----------

from pyspark.sql.functions import *

# ==========================================
# Task 6 : Standardize Supplier Data
# ==========================================

supplier_df = (

    supplier_df

    # ======================================
    # Cast ID Columns
    # ======================================
    .withColumn("supplierid", col("supplierid").cast("double").cast("int"))
    .withColumn("suppliercategoryid", col("suppliercategoryid").cast("double").cast("int"))
    .withColumn("primarycontactpersonid", col("primarycontactpersonid").cast("double").cast("int"))
    .withColumn("alternatecontactpersonid", col("alternatecontactpersonid").cast("double").cast("int"))
    .withColumn("deliverycityid", col("deliverycityid").cast("double").cast("int"))
    .withColumn("postalcityid", col("postalcityid").cast("double").cast("int"))
    .withColumn("deliverymethodid", col("deliverymethodid").cast("double").cast("int"))
    .withColumn("paymentdays", col("paymentdays").cast("double").cast("int"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    # ======================================
    # Trim Text Columns
    # ======================================
    .withColumn("suppliername", trim(col("suppliername")))
    .withColumn("supplierreference", trim(col("supplierreference")))
    .withColumn("websiteurl", trim(col("websiteurl")))
    .withColumn("phonenumber", trim(col("phonenumber")))
    .withColumn("faxnumber", trim(col("faxnumber")))
    .withColumn("deliveryaddressline1", trim(col("deliveryaddressline1")))
    .withColumn("deliveryaddressline2", trim(col("deliveryaddressline2")))
    .withColumn("postaladdressline1", trim(col("postaladdressline1")))
    .withColumn("postaladdressline2", trim(col("postaladdressline2")))
    .withColumn("bankaccountname", trim(col("bankaccountname")))
    .withColumn("bankaccountbranch", trim(col("bankaccountbranch")))
    .withColumn("bankaccountcode", trim(col("bankaccountcode")))
    .withColumn("bankaccountnumber", trim(col("bankaccountnumber")))
    .withColumn("bankinternationalcode", trim(col("bankinternationalcode")))
    .withColumn("internalcomments", trim(col("internalcomments")))

    # ======================================
    # Title Case
    # ======================================
    .withColumn("suppliername", initcap(col("suppliername")))

    # ======================================
    # Clean Postal Codes
    # ======================================
    .withColumn(
        "postalpostalcode",
        regexp_replace(trim(col("postalpostalcode")), "\\s+", "")
    )

    .withColumn(
        "deliverypostalcode",
        regexp_replace(trim(col("deliverypostalcode")), "\\s+", "")
    )

    # ======================================
    # Email Validation
    # (Supplier table has no email)
    # ======================================
    .withColumn("is_valid_email", lit(True))

    # ======================================
    # Phone Validation
    # ======================================
    .withColumn(
        "is_valid_phone",
        col("phonenumber").rlike("^[0-9 +()-]{6,20}$")
    )

)

# COMMAND ----------

# Standardize Supplier Category Lookup
from pyspark.sql.functions import *

supplier_category_df = (

    supplier_category_df

    .withColumn(
        "suppliercategoryid",
        col("suppliercategoryid").cast("double").cast("int")
    )

    .withColumn(
        "suppliercategoryname",
        initcap(trim(col("suppliercategoryname")))
    )

    .withColumn(
        "lasteditedby",
        col("lasteditedby").cast("double").cast("int")
    )

)

# COMMAND ----------

# Standardize People Lookup
from pyspark.sql.functions import *

person_df = (

    person_df

    .withColumn(
        "personid",
        col("personid").cast("double").cast("int")
    )

    .withColumn(
        "fullname",
        initcap(trim(col("fullname")))
    )

    .withColumn(
        "preferredname",
        initcap(trim(col("preferredname")))
    )

    .withColumn(
        "emailaddress",
        trim(lower(col("emailaddress")))
    )

    .withColumn(
        "phonenumber",
        trim(col("phonenumber"))
    )

    .withColumn(
        "is_valid_email",
        col("emailaddress").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")
    )

    .withColumn(
        "is_valid_phone",
        col("phonenumber").rlike("^[0-9 +()-]{6,20}$")
    )

    .withColumn(
        "lasteditedby",
        col("lasteditedby").cast("double").cast("int")
    )

)

# COMMAND ----------

supplier_df.printSchema()

supplier_category_df.printSchema()

person_df.printSchema()

# COMMAND ----------

display(
    supplier_df.select(
        "supplierid",
        "suppliername",
        "suppliercategoryid",
        "primarycontactpersonid",
        "alternatecontactpersonid",
        "phonenumber",
        "deliverypostalcode",
        "postalpostalcode"
    )
)

display(
    supplier_category_df.select(
        "suppliercategoryid",
        "suppliercategoryname"
    )
)

display(
    person_df.select(
        "personid",
        "fullname",
        "preferredname",
        "emailaddress",
        "phonenumber",
        "is_valid_email",
        "is_valid_phone"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 7 : Perform pre-join DQ checks: count total records, validate primary keys for NULLs and invalid values, validate required fields, check lookup table keys

# COMMAND ----------

# ==========================================
# Total Records
# ==========================================

total_records = supplier_df.count()

print("Total Supplier Records :", total_records)

# COMMAND ----------

# ==========================================
# Primary Key NULL Check
# ==========================================

null_supplierid = supplier_df.filter(
    col("supplierid").isNull()
).count()

print("NULL Supplier ID :", null_supplierid)

log_dq(
    table_name="purchasing_suppliers",
    column_name="supplierid",
    dq_type="NULL_CHECK",
    dq_description="Supplier ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Primary Key NULL Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_supplierid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Invalid Supplier ID
# ==========================================

invalid_supplierid = supplier_df.filter(
    col("supplierid") <= 0
).count()

print("Invalid Supplier ID :", invalid_supplierid)

log_dq(
    table_name="purchasing_suppliers",
    column_name="supplierid",
    dq_type="INVALID_VALUE",
    dq_description="Supplier ID should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Primary Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=invalid_supplierid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Duplicate Supplier ID
# ==========================================

duplicate_supplierid = (
    supplier_df.groupBy("supplierid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Supplier IDs :", duplicate_supplierid)

log_dq(
    table_name="purchasing_suppliers",
    column_name="supplierid",
    dq_type="DUPLICATE_CHECK",
    dq_description="Supplier ID should be unique",
    check_category="PRE_JOIN",
    check_name="Duplicate Primary Key Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=duplicate_supplierid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Mandatory Supplier Name
# ==========================================

missing_supplier_name = supplier_df.filter(
    col("suppliername").isNull() |
    (trim(col("suppliername")) == "")
).count()

print("Missing Supplier Name :", missing_supplier_name)

log_dq(
    table_name="purchasing_suppliers",
    column_name="suppliername",
    dq_type="MANDATORY_FIELD",
    dq_description="Supplier Name should not be NULL or blank",
    check_category="PRE_JOIN",
    check_name="Mandatory Field Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_supplier_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Supplier Category Lookup Check
# ==========================================

invalid_category = (
    supplier_df.alias("s")
    .join(
        supplier_category_df.alias("c"),
        col("s.suppliercategoryid") == col("c.suppliercategoryid"),
        "left"
    )
    .filter(col("c.suppliercategoryid").isNull())
    .count()
)

print("Invalid Supplier Category IDs :", invalid_category)

log_dq(
    table_name="purchasing_suppliers",
    column_name="suppliercategoryid",
    dq_type="LOOKUP_KEY",
    dq_description="Supplier Category ID should exist in Supplier Categories",
    check_category="PRE_JOIN",
    check_name="Lookup Key Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_category,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Primary Contact Lookup Check
# ==========================================

invalid_primary_contact = (
    supplier_df.alias("s")
    .join(
        person_df.alias("p"),
        col("s.primarycontactpersonid") == col("p.personid"),
        "left"
    )
    .filter(col("p.personid").isNull())
    .count()
)

print("Invalid Primary Contact IDs :", invalid_primary_contact)

log_dq(
    table_name="purchasing_suppliers",
    column_name="primarycontactpersonid",
    dq_type="LOOKUP_KEY",
    dq_description="Primary Contact Person ID should exist in People",
    check_category="PRE_JOIN",
    check_name="Lookup Key Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_primary_contact,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Alternate Contact Lookup Check
# ==========================================

invalid_alternate_contact = (
    supplier_df.alias("s")
    .join(
        person_df.alias("p"),
        col("s.alternatecontactpersonid") == col("p.personid"),
        "left"
    )
    .filter(col("p.personid").isNull())
    .count()
)

print("Invalid Alternate Contact IDs :", invalid_alternate_contact)

log_dq(
    table_name="purchasing_suppliers",
    column_name="alternatecontactpersonid",
    dq_type="LOOKUP_KEY",
    dq_description="Alternate Contact Person ID should exist in People",
    check_category="PRE_JOIN",
    check_name="Lookup Key Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_alternate_contact,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 8 : Log all pre-join DQ results to centralized logger with table name, column name, check type, severity, total/failed counts, pipeline_run_id

# COMMAND ----------

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

# MAGIC %md
# MAGIC #### Task 9 : Filter records passing pre-join checks, remove failing records

# COMMAND ----------

from pyspark.sql.functions import col, trim

# ==========================================
# Task 9 : Filter Valid Supplier Records
# ==========================================

supplier_valid_df = (

    supplier_df.alias("s")

    # Validate Supplier Category
    .join(
        supplier_category_df.alias("sc"),
        col("s.suppliercategoryid") == col("sc.suppliercategoryid"),
        "left"
    )

    # Validate Primary Contact
    .join(
        person_df.alias("pp"),
        col("s.primarycontactpersonid") == col("pp.personid"),
        "left"
    )

    # Validate Alternate Contact
    .join(
        person_df.alias("ap"),
        col("s.alternatecontactpersonid") == col("ap.personid"),
        "left"
    )

    # Keep only valid records
    .filter(col("s.supplierid").isNotNull())
    .filter(col("s.supplierid") > 0)

    .filter(col("s.suppliername").isNotNull())
    .filter(trim(col("s.suppliername")) != "")

    .filter(col("sc.suppliercategoryid").isNotNull())
    .filter(col("pp.personid").isNotNull())
    .filter(col("ap.personid").isNotNull())

    .select("s.*")

)

# COMMAND ----------

print("Original Records :", supplier_df.count())
print("Valid Records    :", supplier_valid_df.count())
print("Rejected Records :", supplier_df.count() - supplier_valid_df.count())

# COMMAND ----------

display(
    supplier_valid_df.select(
        "supplierid",
        "suppliername",
        "suppliercategoryid",
        "primarycontactpersonid",
        "alternatecontactpersonid"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 10 : Prepare lookup tables: remove duplicates, ensure unique primary keys before joining

# COMMAND ----------

# ==========================================
# Task 10.1 : Prepare Supplier Category Lookup
# ==========================================

supplier_category_lookup_df = (
    supplier_category_df
    .dropDuplicates(["suppliercategoryid"])
)

print("Original Supplier Category Records :", supplier_category_df.count())
print("Unique Supplier Category Records   :", supplier_category_lookup_df.count())

# COMMAND ----------

# ==========================================
# Task 10.2 : Prepare People Lookup
# ==========================================

person_lookup_df = (
    person_df
    .dropDuplicates(["personid"])
)

print("Original People Records :", person_df.count())
print("Unique People Records   :", person_lookup_df.count())

# COMMAND ----------

display(
    supplier_category_lookup_df.select(
        "suppliercategoryid",
        "suppliercategoryname"
    )
)

display(
    person_lookup_df.select(
        "personid",
        "fullname",
        "preferredname"
    )
)

print("Supplier Category Lookup :", supplier_category_lookup_df.count())
print("People Lookup            :", person_lookup_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 11 : Join main table with lookup tables using left joins to avoid data loss

# COMMAND ----------

from pyspark.sql.functions import col

# ==========================================
# Join Supplier Category Lookup
# ==========================================

supplier_category_join_df = (
    supplier_valid_df.alias("s")
    .join(
        supplier_category_lookup_df.alias("sc"),
        col("s.suppliercategoryid") == col("sc.suppliercategoryid"),
        "left"
    )
    .select(
        col("s.*"),
        col("sc.suppliercategoryname").alias("supplier_category_name")
    )
)

# COMMAND ----------

print("Records After Supplier Category Join :", supplier_category_join_df.count())

display(
    supplier_category_join_df.select(
        "supplierid",
        "suppliername",
        "suppliercategoryid",
        "supplier_category_name"
    )
)

# COMMAND ----------

# ==========================================
# Join Primary Contact Lookup
# ==========================================

supplier_primary_contact_df = (
    supplier_category_join_df.alias("s")
    .join(
        person_lookup_df.alias("p"),
        col("s.primarycontactpersonid") == col("p.personid"),
        "left"
    )
    .select(
        col("s.*"),
        col("p.fullname").alias("primary_contact_name")
    )
)

# COMMAND ----------

print("Records After Primary Contact Join :", supplier_primary_contact_df.count())

display(
    supplier_primary_contact_df.select(
        "supplierid",
        "suppliername",
        "primarycontactpersonid",
        "primary_contact_name"
    )
)


# COMMAND ----------

# ==========================================
# Join Alternate Contact Lookup
# ==========================================

supplier_join_df = (
    supplier_primary_contact_df.alias("s")
    .join(
        person_lookup_df.alias("p"),
        col("s.alternatecontactpersonid") == col("p.personid"),
        "left"
    )
    .select(
        col("s.*"),
        col("p.fullname").alias("alternate_contact_name")
    )
)

# COMMAND ----------

print("Records After Alternate Contact Join :", supplier_join_df.count())

display(
    supplier_join_df.select(
        "supplierid",
        "suppliername",
        "supplier_category_name",
        "primary_contact_name",
        "alternate_contact_name"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 12 : Perform post-join DQ checks: validate mandatory fields, referential integrity for lookup names, capture failed counts, log to DQ logger

# COMMAND ----------

from pyspark.sql.functions import col, trim

# ==========================================
# Post-Join Check : Supplier Category Name
# ==========================================

total_records = supplier_join_df.count()

missing_supplier_category = supplier_join_df.filter(
    col("supplier_category_name").isNull() |
    (trim(col("supplier_category_name")) == "")
).count()

print("Missing Supplier Category Name :", missing_supplier_category)

log_dq(
    table_name="purchasing_suppliers",
    column_name="supplier_category_name",
    dq_type="POST_JOIN",
    dq_description="Supplier Category Name should not be NULL after lookup join",
    check_category="POST_JOIN",
    check_name="Supplier Category Lookup Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_supplier_category,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Post-Join Check : Primary Contact Name
# ==========================================

missing_primary_contact = supplier_join_df.filter(
    col("primary_contact_name").isNull() |
    (trim(col("primary_contact_name")) == "")
).count()

print("Missing Primary Contact Name :", missing_primary_contact)

log_dq(
    table_name="purchasing_suppliers",
    column_name="primary_contact_name",
    dq_type="POST_JOIN",
    dq_description="Primary Contact Name should not be NULL after lookup join",
    check_category="POST_JOIN",
    check_name="Primary Contact Lookup Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_primary_contact,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Post-Join Check : Alternate Contact Name
# ==========================================

missing_alternate_contact = supplier_join_df.filter(
    col("alternate_contact_name").isNull() |
    (trim(col("alternate_contact_name")) == "")
).count()

print("Missing Alternate Contact Name :", missing_alternate_contact)

log_dq(
    table_name="purchasing_suppliers",
    column_name="alternate_contact_name",
    dq_type="POST_JOIN",
    dq_description="Alternate Contact Name should not be NULL after lookup join",
    check_category="POST_JOIN",
    check_name="Alternate Contact Lookup Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_alternate_contact,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

spark.table(dq_logger_table) \
    .filter(col("pipeline_run_id") == pipeline_run_id) \
    .select(
        "table_name",
        "column_name",
        "dq_type",
        "failed_records",
        "dq_pass_rate"
    ) \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 13 : Filter final records removing post-join DQ failures

# COMMAND ----------

from pyspark.sql.functions import col, trim

# ==========================================
# Task 13 : Filter Final Valid Supplier Records
# ==========================================

supplier_final_df = (
    supplier_join_df

    # Supplier Category Name Validation
    .filter(col("supplier_category_name").isNotNull())
    .filter(trim(col("supplier_category_name")) != "")

    # Primary Contact Name Validation
    .filter(col("primary_contact_name").isNotNull())
    .filter(trim(col("primary_contact_name")) != "")

    # Alternate Contact Name Validation
    .filter(col("alternate_contact_name").isNotNull())
    .filter(trim(col("alternate_contact_name")) != "")
)

# COMMAND ----------

print("Records After Join       :", supplier_join_df.count())
print("Final Valid Records      :", supplier_final_df.count())
print("Rejected After Post Join :", supplier_join_df.count() - supplier_final_df.count())

# COMMAND ----------

display(
    supplier_final_df.select(
        "supplierid",
        "suppliername",
        "supplier_category_name",
        "primary_contact_name",
        "alternate_contact_name"
    )
)

# COMMAND ----------

spark.table(silver_supplier_table).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 14 : Align final dataset with Silver table schema: select required business columns, prefix IDs (e.g., wwi_customer_id), add metadata columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# ==========================================
# Task 14 : Align with Silver Schema
# ==========================================

supplier_silver_df = (

    supplier_final_df.select(

        # Business Columns
        col("supplierid").alias("wwi_supplier_id"),
        col("suppliername").alias("supplier_name"),
        col("supplier_category_name").alias("category"),
        col("websiteurl").alias("website_url"),
        col("primary_contact_name").alias("primary_contact"),
        col("supplierreference").alias("supplier_reference"),
        col("paymentdays").alias("payment_days"),
        col("postalpostalcode").alias("postal_code"),

        # Metadata Columns
        current_timestamp().alias("created_timestamp"),
        current_timestamp().alias("last_updated_timestamp"),
        lit(pipeline_run_id).alias("pipeline_run_id"),
        lit(business_month).alias("business_month")
    )

)

# COMMAND ----------

supplier_silver_df.printSchema()

display(supplier_silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 15 : Delta MERGE into Silver table using business key: update matching records with latest values and last_updated_timestamp, insert new records with all columns

# COMMAND ----------

from delta.tables import DeltaTable

# ==========================================
# Task 15 : Delta MERGE into Silver Table
# ==========================================

# Get Silver Delta Table Reference
silver_delta_table = DeltaTable.forName(
    spark,
    silver_supplier_table
)

# Perform SCD Type 1 MERGE
(
    silver_delta_table.alias("target")
    .merge(
        supplier_silver_df.alias("source"),
        """
        target.wwi_supplier_id = source.wwi_supplier_id
        AND target.business_month = source.business_month
        """
    )

    # Update existing records
    .whenMatchedUpdate(
        set={
            "supplier_name": "source.supplier_name",
            "category": "source.category",
            "website_url": "source.website_url",
            "primary_contact": "source.primary_contact",
            "supplier_reference": "source.supplier_reference",
            "payment_days": "source.payment_days",
            "postal_code": "source.postal_code",
            "business_month": "source.business_month",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    # Insert new records
    .whenNotMatchedInsert(
        values={
            "wwi_supplier_id": "source.wwi_supplier_id",
            "supplier_name": "source.supplier_name",
            "category": "source.category",
            "website_url": "source.website_url",
            "primary_contact": "source.primary_contact",
            "supplier_reference": "source.supplier_reference",
            "payment_days": "source.payment_days",
            "postal_code": "source.postal_code",
            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id",
            "business_month": "source.business_month"
        }
    )

    .execute()
)

# COMMAND ----------

print("Silver Supplier Table Count :")
print(spark.table(silver_supplier_table).count())

display(
    spark.table(silver_supplier_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 16 : Display execution summary: total Silver records, completion message, pipeline_run_id, business_month

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

silver_record_count = spark.table(silver_supplier_table).count()

print("=" * 60)
print("Supplier Staging Pipeline Completed Successfully")
print("=" * 60)

print(f"Business Month        : {business_month}")
print(f"Pipeline Run ID       : {pipeline_run_id}")
print(f"Total Silver Records  : {silver_record_count}")

print("=" * 60)
print(f"Data successfully loaded into: {silver_supplier_table}")
print("=" * 60)

# COMMAND ----------

display(
    spark.table(silver_supplier_table)
)

# COMMAND ----------

