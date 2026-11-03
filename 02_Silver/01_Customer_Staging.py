# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_Common_Functions

# COMMAND ----------

# Created Business_Month Widget
dbutils.widgets.text(
    "business_month",
    "June 2016",
    "Business Month"
)

business_month = dbutils.widgets.get("business_month").strip()

if business_month == "":
    raise Exception("Business Month is mandatory.")
print("business_month: ", business_month)

# COMMAND ----------

# Imports & Notebook Setup
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

pipeline_run_id = str(uuid.uuid4())

print("Pipeline Run ID:", pipeline_run_id)

# COMMAND ----------

# Verify Logger Table
dq_logger_table = "wwi.logger.dq_logger"

verify_logger_table(dq_logger_table)

# COMMAND ----------

# Define Source & Target Tables
# Bronze Source Tables

customer_table = "wwi.bronze.sales_customers"

customer_category_table = "wwi.bronze.sales_customercategories"

buying_group_table = "wwi.bronze.sales_buyinggroups"

city_table = "wwi.bronze.application_cities"

# Silver Target Table

silver_table = "wwi.silver.customer_staging"

# COMMAND ----------

from datetime import datetime

business_month = dbutils.widgets.get("business_month").strip()

if business_month == "":
    raise Exception("Business Month is mandatory.")

# Convert "June 2016" -> "2016-06"
business_month_filter = datetime.strptime(
    business_month,
    "%B %Y"
).strftime("%Y-%m")

print("Widget Value :", business_month)
print("Filter Value :", business_month_filter)

# COMMAND ----------

# Load Bronze Tables
customer_df = (
    spark.table(customer_table)
    .filter(col("business_month") == business_month_filter)
)

category_df = (
    spark.table(customer_category_table)
    .filter(col("business_month") == business_month_filter)
)

buying_group_df = (
    spark.table(buying_group_table)
    .filter(col("business_month") == business_month_filter)
)

city_df = (
    spark.table(city_table)
    .filter(col("business_month") == business_month_filter)
)

# COMMAND ----------

# Validate Input data
print("Customer Records :", customer_df.count())
print("Category Records :", category_df.count())
print("Buying Group Records :", buying_group_df.count())
print("City Records :", city_df.count())

if customer_df.count() == 0:
    raise Exception("No Customer records found for selected Business Month.")

# COMMAND ----------

from pyspark.sql.functions import *

customer_df = (
    customer_df

    # ======================================================
    # Cast ID Columns
    # ======================================================
    .withColumn("customerid", col("customerid").cast("double").cast("int"))
    .withColumn("billtocustomerid", col("billtocustomerid").cast("double").cast("int"))
    .withColumn("buyinggroupid", col("buyinggroupid").cast("double").cast("int"))
    .withColumn("customercategoryid", col("customercategoryid").cast("double").cast("int"))
    .withColumn("deliverycityid", col("deliverycityid").cast("double").cast("int"))
    .withColumn("postalcityid", col("postalcityid").cast("double").cast("int"))
    .withColumn("primarycontactpersonid", col("primarycontactpersonid").cast("double").cast("int"))
    .withColumn("alternatecontactpersonid", col("alternatecontactpersonid").cast("double").cast("int"))
    .withColumn("deliverymethodid", col("deliverymethodid").cast("double").cast("int"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    # ======================================================
    # Trim String Columns
    # ======================================================
    .withColumn("customername", trim(col("customername")))
    .withColumn("deliveryaddressline1", trim(col("deliveryaddressline1")))
    .withColumn("deliveryaddressline2", trim(col("deliveryaddressline2")))
    .withColumn("postaladdressline1", trim(col("postaladdressline1")))
    .withColumn("postaladdressline2", trim(col("postaladdressline2")))
    .withColumn("websiteurl", trim(col("websiteurl")))
    .withColumn("phonenumber", trim(col("phonenumber")))
    .withColumn("faxnumber", trim(col("faxnumber")))

    # ======================================================
    # Standardize Customer Name
    # ======================================================
    .withColumn("customername", initcap(col("customername")))

    # ======================================================
    # Clean Postal Codes
    # ======================================================
    .withColumn(
        "postalpostalcode",
        upper(regexp_replace(trim(col("postalpostalcode")), "\\s+", ""))
    )
    .withColumn(
        "deliverypostalcode",
        upper(regexp_replace(trim(col("deliverypostalcode")), "\\s+", ""))
    )

    # ======================================================
    # Standardize Phone Number
    # ======================================================
    # Remove decimal (.0)
    .withColumn(
        "phonenumber",
        regexp_replace(col("phonenumber"), "\\.0$", "")
    )

    # Remove everything except digits
    .withColumn(
        "phonenumber",
        regexp_replace(col("phonenumber"), "[^0-9]", "")
    )

    # Keep last 10 digits
    .withColumn(
        "phonenumber",
        expr("right(phonenumber,10)")
    )

    # ======================================================
    # Standardize Fax Number
    # ======================================================
    .withColumn(
        "faxnumber",
        regexp_replace(col("faxnumber"), "\\.0$", "")
    )
    .withColumn(
        "faxnumber",
        regexp_replace(col("faxnumber"), "[^0-9]", "")
    )

    # ======================================================
    # Email Validation
    # ======================================================
    .withColumn("is_valid_email", lit(True))

    # ======================================================
    # Phone Validation
    # ======================================================
    .withColumn(
        "is_valid_phone",
        col("phonenumber").rlike("^[6-9][0-9]{7}$")
    )

    # ======================================================
    # Fax Validation
    # ======================================================
    .withColumn(
        "is_valid_fax",
        col("faxnumber").rlike("^[0-9]{6,15}$")
    )
)

# COMMAND ----------

display(customer_df.select(
    "customerid",
    "customername",
    "phonenumber",
    "postalpostalcode",
    "is_valid_phone"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perform pre-join DQ checks: count total records, validate primary keys for NULLs and invalid values, validate required fields, check lookup table keys

# COMMAND ----------

# Total Records
total_records = customer_df.count()

print(f"Total Customer Records : {total_records}")

# COMMAND ----------

# Null Customer ID
null_customerid = customer_df.filter(
    col("customerid").isNull()
).count()

print("Null Customer ID :", null_customerid)

# COMMAND ----------

# Log the result
log_dq(
    table_name="sales_customers",
    column_name="customerid",
    dq_type="NULL_CHECK",
    dq_description="Customer ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="NULL Customer ID",
    severity="CRITICAL",
    total_records=total_records,
    failed_records=null_customerid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

spark.table("wwi.logger.dq_logger").printSchema()

# COMMAND ----------

spark.table("wwi.logger.dq_logger").show(5, truncate=False)

# COMMAND ----------

# Invalid Customer ID
invalid_customerid = customer_df.filter(
    col("customerid") <= 0
).count()

print("Invalid Customer ID :", invalid_customerid)

log_dq(
    table_name="sales_customers",
    column_name="customerid",
    dq_type="INVALID_VALUE",
    dq_description="Customer ID should be greater than 0",
    check_category="PRE_JOIN",
    check_name="Invalid Customer ID",
    severity="CRITICAL",
    total_records=total_records,
    failed_records=invalid_customerid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Duplicate Customer ID
duplicate_customerid = (
    customer_df
    .groupBy("customerid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Customer ID :", duplicate_customerid)

log_dq(
    table_name="sales_customers",
    column_name="customerid",
    dq_type="DUPLICATE_CHECK",
    dq_description="Customer ID should be unique",
    check_category="PRE_JOIN",
    check_name="Duplicate Customer ID",
    severity="CRITICAL",
    total_records=total_records,
    failed_records=duplicate_customerid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Mandatory Customer Name
blank_customername = customer_df.filter(
    col("customername").isNull() |
    (trim(col("customername")) == "")
).count()

print("Blank Customer Name :", blank_customername)

log_dq(
    table_name="sales_customers",
    column_name="customername",
    dq_type="MANDATORY_FIELD",
    dq_description="Customer Name should not be NULL or blank",
    check_category="PRE_JOIN",
    check_name="Customer Name Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=blank_customername,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Phone Number Validation
invalid_phone = customer_df.filter(
    col("is_valid_phone")
).count()

print("Invalid Phone Numbers :", invalid_phone)

log_dq(
    table_name="sales_customers",
    column_name="phonenumber",
    dq_type="REGEX_VALIDATION",
    dq_description="Phone number format is invalid",
    check_category="PRE_JOIN",
    check_name="Phone Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_phone,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Customer Category Lookup Key
null_category = customer_df.filter(
    col("customercategoryid").isNull()
).count()

print("Null Customer Category :", null_category)

log_dq(
    table_name="sales_customers",
    column_name="customercategoryid",
    dq_type="LOOKUP_KEY",
    dq_description="Customer Category ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Customer Category Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_category,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Buying Group Lookup Key
null_buyinggroup = customer_df.filter(
    col("buyinggroupid").isNull()
).count()

print("Null Buying Group :", null_buyinggroup)

log_dq(
    table_name="sales_customers",
    column_name="buyinggroupid",
    dq_type="LOOKUP_KEY",
    dq_description="Buying Group ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Buying Group Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=null_buyinggroup,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Delivery City Lookup Key
null_deliverycity = customer_df.filter(
    col("deliverycityid").isNull()
).count()

print("Null Delivery City :", null_deliverycity)

log_dq(
    table_name="sales_customers",
    column_name="deliverycityid",
    dq_type="LOOKUP_KEY",
    dq_description="Delivery City ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Delivery City Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_deliverycity,
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
        "severity",
        "total_records",
        "failed_records",
        "issue_percentage",
        "dq_pass_rate",
        "pipeline_run_id"
    ) \
    .show(truncate=False)

# COMMAND ----------

# =====================================================
# Task 9 : Filter Valid Customer Records
# =====================================================

customer_valid_df = (
    customer_df
    .filter(col("customerid").isNotNull())
    .filter(col("customerid") > 0)
    .filter(col("customername").isNotNull())
    .filter(trim(col("customername")) != "")
    .filter(col("customercategoryid").isNotNull())
    .filter(col("deliverycityid").isNotNull())
)

# COMMAND ----------

# Compare Before & After
print("Original Customer Records :", customer_df.count())

print("Valid Customer Records :", customer_valid_df.count())

print(
    "Rejected Customer Records :",
    customer_df.count() - customer_valid_df.count()
)

# COMMAND ----------

# Verify the filtered data
display(customer_valid_df.select(
    "customerid",
    "customername",
    "customercategoryid",
    "buyinggroupid",
    "deliverycityid"
).limit(10))

# COMMAND ----------

rejected_customer_df = customer_df.subtract(customer_valid_df)

print("Rejected Records")

display(rejected_customer_df)

# COMMAND ----------

# Customer Category Lookup
# ==========================================
# Prepare Customer Category Lookup
# ==========================================

category_lookup_df = (
    category_df
    .select(
        col("customercategoryid").cast("int"),
        trim(col("customercategoryname")).alias("customer_category_name")
    )
    .dropDuplicates(["customercategoryid"])
)

print("Customer Category Records :", category_lookup_df.count())
display(category_lookup_df)

# COMMAND ----------

# ==========================================
# Prepare Buying Group Lookup
# ==========================================

buyinggroup_lookup_df = (
    buying_group_df
    .select(
        col("buyinggroupid").cast("int"),
        trim(col("buyinggroupname")).alias("buying_group_name")
    )
    .dropDuplicates(["buyinggroupid"])
)

print("Buying Group Records :", buyinggroup_lookup_df.count())
display(buyinggroup_lookup_df)

# COMMAND ----------

# ==========================================
# Prepare City Lookup
# ==========================================

city_lookup_df = (
    city_df
    .select(
        col("cityid").cast("int"),
        trim(col("cityname")).alias("city_name"),
        col("stateprovinceid").cast("int")
    )
    .dropDuplicates(["cityid"])
)

print("City Records :", city_lookup_df.count())
display(city_lookup_df)

# COMMAND ----------

category_lookup_df.groupBy("customercategoryid") \
    .count() \
    .filter(col("count") > 1) \
    .show()

buyinggroup_lookup_df.groupBy("buyinggroupid") \
    .count() \
    .filter(col("count") > 1) \
    .show()

city_lookup_df.groupBy("cityid") \
    .count() \
    .filter(col("count") > 1) \
    .show()

# COMMAND ----------

category_df.printSchema()

buying_group_df.printSchema()

city_df.printSchema()

# COMMAND ----------

customer_join_df = (
    customer_valid_df.alias("c")
    .join(
        category_lookup_df.alias("cc"),
        col("c.customercategoryid") == col("cc.customercategoryid"),
        "left"
    )
    .join(
        buyinggroup_lookup_df.alias("bg"),
        col("c.buyinggroupid") == col("bg.buyinggroupid"),
        "left"
    )
    .join(
        city_lookup_df.alias("ct"),
        col("c.deliverycityid") == col("ct.cityid"),
        "left"
    )
    .select(
        col("c.*"),
        col("cc.customer_category_name"),
        col("bg.buying_group_name"),
        col("ct.city_name")
    )
)

# COMMAND ----------

print("Customer Records:", customer_join_df.count())

display(
    customer_join_df.select(
        "customerid",
        "customername",
        "customer_category_name",
        "buying_group_name",
        "city_name"
    )
)

# COMMAND ----------

category_lookup_df.printSchema()

buyinggroup_lookup_df.printSchema()

city_lookup_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 12 : Post-Join DQ Checks
# ==========================================

post_join_total_records = customer_join_df.count()

print("Post Join Total Records :", post_join_total_records)

# COMMAND ----------

# Customer Category Name Validation
null_category_name = customer_join_df.filter(
    col("customer_category_name").isNull()
).count()

print("Missing Customer Category Name :", null_category_name)

log_dq(
    table_name="sales_customers",
    column_name="customer_category_name",
    dq_type="REFERENTIAL_INTEGRITY",
    dq_description="Customer Category Name missing after lookup join",
    check_category="POST_JOIN",
    check_name="Customer Category Lookup",
    severity="HIGH",
    total_records=post_join_total_records,
    failed_records=null_category_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# City Name Validation
null_city_name = customer_join_df.filter(
    col("city_name").isNull()
).count()

print("Missing City Name :", null_city_name)

log_dq(
    table_name="sales_customers",
    column_name="city_name",
    dq_type="REFERENTIAL_INTEGRITY",
    dq_description="City Name missing after lookup join",
    check_category="POST_JOIN",
    check_name="City Lookup",
    severity="HIGH",
    total_records=post_join_total_records,
    failed_records=null_city_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Buying Group Name Validation
missing_buying_group = customer_join_df.filter(
    col("buyinggroupid").isNotNull() &
    col("buying_group_name").isNull()
).count()

print("Missing Buying Group Name :", missing_buying_group)

log_dq(
    table_name="sales_customers",
    column_name="buying_group_name",
    dq_type="REFERENTIAL_INTEGRITY",
    dq_description="Buying Group Name missing after lookup join",
    check_category="POST_JOIN",
    check_name="Buying Group Lookup",
    severity="MEDIUM",
    total_records=post_join_total_records,
    failed_records=missing_buying_group,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Verify Post-Join DQ Logs
display(spark.table(dq_logger_table) \
    .filter(
        (col("pipeline_run_id") == pipeline_run_id) &
        (col("check_category") == "POST_JOIN")
    ) \
    .select(
        "table_name",
        "column_name",
        "check_name",
        "severity",
        "failed_records",
        "dq_pass_rate"
    ) \
    .limit(10))

# COMMAND ----------

# =====================================================
# Task 13 : Filter Final Records
# =====================================================

customer_final_df = (
    customer_join_df
    .filter(col("customer_category_name").isNotNull())
    .filter(col("city_name").isNotNull())
)

# COMMAND ----------

# Compare Counts
print("Post Join Records :", customer_join_df.count())

print("Final Records :", customer_final_df.count())

print(
    "Rejected Records :",
    customer_join_df.count() - customer_final_df.count()
)

# COMMAND ----------

# Display Final Data
display(
    customer_final_df.select(
        "customerid",
        "customername",
        "customer_category_name",
        "buying_group_name",
        "city_name"
    )
)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# =====================================================
# Task 14 : Align Final Dataset with Silver Schema
# =====================================================

customer_silver_df = (
    customer_final_df.select(

        # Business Key
        col("customerid").alias("wwi_customer_id"),

        # Business Columns
        col("customername").alias("customer"),

        col("billtocustomerid").alias("bill_to_customer"),

        col("customer_category_name").alias("category"),

        col("buying_group_name").alias("buying_group"),

        col("primarycontactpersonid").alias("primary_contact"),

        col("postalpostalcode").alias("postal_code"),

        # Metadata
        lit(business_month).alias("business_month"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp"),

        lit(pipeline_run_id).alias("pipeline_run_id")
    )
)

# COMMAND ----------

customer_silver_df.printSchema()

# COMMAND ----------

customer_silver_df.show(10, truncate=False)

# COMMAND ----------

# Verify metadata columns
display(customer_silver_df.select(
    "wwi_customer_id",
    "created_timestamp",
    "last_updated_timestamp",
    "business_month"
).limit(10))

# COMMAND ----------

spark.table(silver_table).printSchema()

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Get Silver Table Reference
silver_delta_table = DeltaTable.forName(
    spark,
    silver_table
)

# COMMAND ----------

# MERGE (SCD Type 1)
(
    silver_delta_table.alias("target")
    .merge(
        customer_silver_df.alias("source"),
        """
        target.wwi_customer_id = source.wwi_customer_id
        AND target.business_month = source.business_month
        """
    )

    # Update existing records
    .whenMatchedUpdate(
        set={
            "customer": "source.customer",
            "bill_to_customer": "source.bill_to_customer",
            "category": "source.category",
            "buying_group": "source.buying_group",
            "primary_contact": "source.primary_contact",
            "postal_code": "source.postal_code",
            "business_month": "source.business_month",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    # Insert new records
    .whenNotMatchedInsert(
        values={
            "wwi_customer_id": "source.wwi_customer_id",
            "customer": "source.customer",
            "bill_to_customer": "source.bill_to_customer",
            "category": "source.category",
            "buying_group": "source.buying_group",
            "primary_contact": "source.primary_contact",
            "postal_code": "source.postal_code",
            "business_month": "source.business_month",
            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    .execute()
)

# COMMAND ----------

# Verify MERGE
print("Silver Customer Table Count :")

spark.table(silver_table).count()

# COMMAND ----------

# Display Silver Table
display(
    spark.table(silver_table)
)

# COMMAND ----------

# Get Total Silver Records
from pyspark.sql.functions import current_timestamp

total_silver_records = spark.table(silver_table).count()

# COMMAND ----------

# Display Summary
print("=" * 70)
print("        CUSTOMER STAGING PIPELINE EXECUTION SUMMARY")
print("=" * 70)

print(f"Pipeline Run ID        : {pipeline_run_id}")
print(f"Business Month         : {business_month}")
print(f"Silver Table           : {silver_table}")
print(f"Total Silver Records   : {total_silver_records}")

print("-" * 70)

print("Pipeline Status        : SUCCESS")
print("Customer Staging completed successfully.")

print("=" * 70)

# COMMAND ----------

    # Create a Summary DataFrame
summary_df = spark.createDataFrame(
    [
        (
            pipeline_run_id,
            business_month,
            silver_table,
            total_silver_records,
            "SUCCESS"
        )
    ],
    [
        "pipeline_run_id",
        "business_month",
        "silver_table",
        "total_silver_records",
        "status"
    ]
)

display(summary_df)

# COMMAND ----------

