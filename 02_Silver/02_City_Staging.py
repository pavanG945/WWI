# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_Common_Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 1: Create business_month input widget (e.g., June 2016), stop execution if missing

# COMMAND ----------

# T1 : Created Business_Month Widget
dbutils.widgets.text (
    "business_month", # internal name of widget
    "2016-06", # Default value
    "Business Month" # Label shown to user
)
business_month = dbutils.widgets.get("business_month").strip()
if business_month == "":
    raise Exception("Business Month is Mandatory")
print("Business Month:", business_month)

# COMMAND ----------

# import and Notebook Setup
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 2: Set up notebook: define Bronze source, Silver target table, DQ logger table, generate unique pipeline_run_id

# COMMAND ----------

import uuid

# Bronze tables
bronze_city_table = "wwi.bronze.application_cities"
bronze_stateprovince_table = "wwi.bronze.application_stateprovinces"
bronze_country_table =  "wwi.bronze.application_countries"

# Silver tables
silver_city_table = "wwi.silver.city_staging"

# DQ Logger
dq_logger_table = "wwi.logger.dq_logger"

# Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("Pipeline Run ID:", pipeline_run_id)

# COMMAND ----------

# Verify Logger Table
dq_logger_table = "wwi.logger.dq_logger"

verify_logger_table(dq_logger_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 3 : Verify central DQ logger table is available in wwi.logger schema

# COMMAND ----------

# Task 3 : Verify DQ Logger

spark.table(dq_logger_table).printSchema()

display(
    spark.table(dq_logger_table).limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 5 : Load Bronze tables required for staging, filtered by business_month (June 2016)

# COMMAND ----------

# Task 5 : Load Bronze Tables

city_df = (
    spark.table(bronze_city_table)
    .filter(col("business_month") == business_month)
)

stateprovince_df = (
    spark.table(bronze_stateprovince_table)
    .filter(col("business_month") == business_month)
)

country_df = (
    spark.table(bronze_country_table)
    .filter(col("business_month") == business_month)
)

# Validate Input Data
print("City Records :", city_df.count())
print("State Province Records :", stateprovince_df.count())
print("Country Records :", country_df.count())

if city_df.count() == 0:
    raise Exception("No City records found for selected Business Month.")

# COMMAND ----------

city_df.printSchema()

stateprovince_df.printSchema()

country_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 6 : Standardize raw Bronze data: cast ID columns to integer, validate email/phone with regex, trim text fields, apply title case to names, clean postal codes

# COMMAND ----------

# Task 6 : Standardize City Data

from pyspark.sql.functions import *

city_df = (
    city_df

    # Cast ID Columns
    .withColumn("cityid", col("cityid").cast("double").cast("int"))
    .withColumn("stateprovinceid", col("stateprovinceid").cast("double").cast("int"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))
    .withColumn("latestrecordedpopulation", col("latestrecordedpopulation").cast("double").cast("long"))

    # Trim Text Columns
    .withColumn("cityname", trim(col("cityname")))
    .withColumn("location", trim(col("location")))

    # Title Case
    .withColumn("cityname", initcap(col("cityname")))

    # Clean Location
    .withColumn(
        "location",
        regexp_replace(col("location"), "\\s+", " ")
    )
)

# COMMAND ----------

# Standardize stateprovince_df
stateprovince_df = (
    stateprovince_df

    # Cast IDs
    .withColumn("stateprovinceid", col("stateprovinceid").cast("double").cast("int"))
    .withColumn("countryid", col("countryid").cast("double").cast("int"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))
    .withColumn("latestrecordedpopulation",
                col("latestrecordedpopulation").cast("double").cast("long"))

    # Trim
    .withColumn("stateprovincename", trim(col("stateprovincename")))
    .withColumn("stateprovincecode", trim(col("stateprovincecode")))
    .withColumn("salesterritory", trim(col("salesterritory")))

    # Title Case
    .withColumn("stateprovincename", initcap(col("stateprovincename")))
)

# COMMAND ----------

# Standardize country_df
country_df = (
    country_df

    # Cast IDs
    .withColumn("countryid", col("countryid").cast("double").cast("int"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))
    .withColumn("latestrecordedpopulation",
                col("latestrecordedpopulation").cast("double").cast("long"))

    # Trim
    .withColumn("countryname", trim(col("countryname")))
    .withColumn("formalname", trim(col("formalname")))
    .withColumn("continent", trim(col("continent")))
    .withColumn("region", trim(col("region")))
    .withColumn("subregion", trim(col("subregion")))

    # Title Case
    .withColumn("countryname", initcap(col("countryname")))
    .withColumn("formalname", initcap(col("formalname")))
    .withColumn("continent", initcap(col("continent")))
)

# COMMAND ----------

# Verify Standarization Data
display(city_df.select(
    "cityid",
    "cityname",
    "stateprovinceid",
    "latestrecordedpopulation",
    "location"
).limit(10))

display(stateprovince_df.select(
    "stateprovinceid",
    "stateprovincename",
    "countryid",
    "salesterritory"
))

display(country_df.select(
    "countryid",
    "countryname",
    "continent",
    "region",
    "subregion"
))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 7 : Perform pre-join DQ checks: count total records, validate primary keys for NULLs and invalid values, validate required fields, check lookup table keys

# COMMAND ----------

# ==========================================
# Task 7 : Pre-Join DQ Checks
# ==========================================

total_records = city_df.count()

print("Total City Records :", total_records)

# COMMAND ----------

# Primary Key NULL Check - Cityid
null_cityid = city_df.filter(
    col("cityid").isNull()
).count()

print("NULL City ID :", null_cityid)

log_dq(
    table_name="application_cities",
    column_name="cityid",
    dq_type="NULL_CHECK",
    dq_description="City ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Primary Key NULL Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_cityid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Invalid City ID Check
invalid_cityid = city_df.filter(
    col("cityid") <= 0
).count()

print("Invalid City ID :", invalid_cityid)

log_dq(
    table_name="application_cities",
    column_name="cityid",
    dq_type="INVALID_VALUE",
    dq_description="City ID should be greater than 0",
    check_category="PRE_JOIN",
    check_name="Primary Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=invalid_cityid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Remove duplicate city IDs
city_df = city_df.dropDuplicates(["cityid"])

print("Records after removing duplicate city IDs:", city_df.count())

# COMMAND ----------

# Duplicate City ID Check
duplicate_cityid = (
    city_df.groupBy("cityid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate City IDs :", duplicate_cityid)

log_dq(
    table_name="application_cities",
    column_name="cityid",
    dq_type="DUPLICATE_CHECK",
    dq_description="Duplicate City IDs",
    check_category="PRE_JOIN",
    check_name="Duplicate Primary Key",
    severity="HIGH",
    total_records=total_records,
    failed_records=duplicate_cityid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Mandatory Field Check - cityname
null_cityname = city_df.filter(
    col("cityname").isNull() |
    (trim(col("cityname")) == "")
).count()

print("NULL City Name :", null_cityname)

log_dq(
    table_name="application_cities",
    column_name="cityname",
    dq_type="MANDATORY_FIELD",
    dq_description="City Name should not be NULL",
    check_category="PRE_JOIN",
    check_name="Mandatory Field Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_cityname,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Mandatory Lookup Key Check stateprovinceid
null_stateprovinceid = city_df.filter(
    col("stateprovinceid").isNull()
).count()

print("NULL State Province ID :", null_stateprovinceid)

log_dq(
    table_name="application_cities",
    column_name="stateprovinceid",
    dq_type="LOOKUP_KEY",
    dq_description="State Province ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Lookup Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_stateprovinceid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Lookup Key Exists in application_stateprovinces
invalid_lookup = (
    city_df.alias("c")
    .join(
        stateprovince_df.alias("s"),
        col("c.stateprovinceid") == col("s.stateprovinceid"),
        "left"
    )
    .filter(col("s.stateprovinceid").isNull())
    .count()
)

print("Invalid State Province Lookup :", invalid_lookup)

log_dq(
    table_name="application_cities",
    column_name="stateprovinceid",
    dq_type="LOOKUP_KEY",
    dq_description="State Province ID not found in lookup table",
    check_category="PRE_JOIN",
    check_name="Lookup Integrity",
    severity="HIGH",
    total_records=total_records,
    failed_records=invalid_lookup,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Verify Pre-Join DQ Results
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
        "dq_pass_rate",
        "pipeline_run_id"
    ) \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 9 : Filter records passing pre-join checks, remove failing records

# COMMAND ----------

# ==========================================
# Task 9 : Filter Valid City Records
# ==========================================

from pyspark.sql.functions import col, trim

city_valid_df = (
    city_df
    .filter(col("cityid").isNotNull())
    .filter(col("cityid") > 0)
    .filter(col("cityname").isNotNull())
    .filter(trim(col("cityname")) != "")
    .filter(col("stateprovinceid").isNotNull())
)

print("Original Records :", city_df.count())
print("Valid Records    :", city_valid_df.count())
print("Rejected Records :", city_df.count() - city_valid_df.count())

# COMMAND ----------

# Verify Filtered Data
display(
    city_valid_df.select(
        "cityid",
        "cityname",
        "stateprovinceid",
        "latestrecordedpopulation"
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 10 : Prepare lookup tables: remove duplicates, ensure unique primary keys before joining

# COMMAND ----------

# Task 10 : Prepare State Province Lookup

stateprovince_lookup_df = (
    stateprovince_df
    .dropDuplicates(["stateprovinceid"])
)

print("Original State Province Records :", stateprovince_df.count())
print("Unique State Province Records   :", stateprovince_lookup_df.count())

# COMMAND ----------

# ==========================================
# Task 10 : Prepare Country Lookup
# ==========================================

country_lookup_df = (
    country_df
    .dropDuplicates(["countryid"])
)

print("Original Country Records :", country_df.count())
print("Unique Country Records   :", country_lookup_df.count())

# COMMAND ----------

display(
    stateprovince_lookup_df.select(
        "stateprovinceid",
        "stateprovincename",
        "countryid",
        "salesterritory"
    ).limit(10)
)

display(
    country_lookup_df.select(
        "countryid",
        "countryname",
        "continent",
        "region",
        "subregion"
    ).limit(10)
)

# COMMAND ----------

duplicate_stateprovince = (
    stateprovince_lookup_df
    .groupBy("stateprovinceid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate State Province IDs :", duplicate_stateprovince)

duplicate_country = (
    country_lookup_df
    .groupBy("countryid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Country IDs :", duplicate_country)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 11 : Join main table with lookup tables using left joins to avoid data loss

# COMMAND ----------

# Task 11 : Join Lookup Tables
# Joiniong Application Cities with application_StateProvince 
city_join_df = (
    city_valid_df.alias("c")

    .join(
        stateprovince_lookup_df.alias("sp"),
        col("c.stateprovinceid") == col("sp.stateprovinceid"),
        "left"
    )
    
    .select(
        col("c.*"),

        col("sp.stateprovincename").alias("state_province_name"),
        col("sp.countryid").alias("countryid"),
        col("sp.salesterritory").alias("sales_territory")
    )
)

# COMMAND ----------

# Again Join with application_countries
city_join_df = (
    city_join_df.alias("c")

    .join(
        country_lookup_df.alias("co"),
        col("c.countryid") == col("co.countryid"),
        "left"
    )

    .select(

        col("c.*"),

        col("co.countryname").alias("country_name"),
        col("co.continent").alias("continent"),
        col("co.region").alias("region"),
        col("co.subregion").alias("subregion")
    )
)

# COMMAND ----------

# Verify Join Results
print("City Records After Join :", city_join_df.count())

display(
    city_join_df.select(
        "cityid",
        "cityname",
        "state_province_name",
        "country_name",
        "continent",
        "sales_territory",
        "region",
        "subregion"
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 12 :  Perform post-join DQ checks: validate mandatory fields, referential integrity for lookup names, capture failed counts, log to DQ logger

# COMMAND ----------

# ==========================================
# Task 12 : Post-Join DQ Checks
# ==========================================

total_records = city_join_df.count()

print("Total Joined Records :", total_records)

# COMMAND ----------

# Check State Province Name
null_state_name = city_join_df.filter(
    col("state_province_name").isNull()
).count()

print("NULL State Province Name :", null_state_name)

log_dq(
    table_name="city_staging",
    column_name="state_province_name",
    dq_type="REFERENTIAL_INTEGRITY",
    dq_description="State Province Name missing after lookup join",
    check_category="POST_JOIN",
    check_name="State Province Lookup Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_state_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Check Country Name
null_country = city_join_df.filter(
    col("country_name").isNull()
).count()

print("NULL Country Name :", null_country)

log_dq(
    table_name="city_staging",
    column_name="country_name",
    dq_type="REFERENTIAL_INTEGRITY",
    dq_description="Country Name missing after lookup join",
    check_category="POST_JOIN",
    check_name="Country Lookup Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_country,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Check Continent
null_continent = city_join_df.filter(
    col("continent").isNull()
).count()

print("NULL Continent :", null_continent)

log_dq(
    table_name="city_staging",
    column_name="continent",
    dq_type="MANDATORY_FIELD",
    dq_description="Continent should not be NULL",
    check_category="POST_JOIN",
    check_name="Mandatory Field Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_continent,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Check Sales Territory
null_sales_territory = city_join_df.filter(
    col("sales_territory").isNull()
).count()

print("NULL Sales Territory :", null_sales_territory)

log_dq(
    table_name="city_staging",
    column_name="sales_territory",
    dq_type="MANDATORY_FIELD",
    dq_description="Sales Territory should not be NULL",
    check_category="POST_JOIN",
    check_name="Mandatory Field Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=null_sales_territory,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Check Region
null_region = city_join_df.filter(
    col("region").isNull()
).count()

print("NULL Region :", null_region)

log_dq(
    table_name="city_staging",
    column_name="region",
    dq_type="MANDATORY_FIELD",
    dq_description="Region should not be NULL",
    check_category="POST_JOIN",
    check_name="Mandatory Field Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=null_region,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Check Subregion
null_subregion = city_join_df.filter(
    col("subregion").isNull()
).count()

print("NULL Subregion :", null_subregion)

log_dq(
    table_name="city_staging",
    column_name="subregion",
    dq_type="MANDATORY_FIELD",
    dq_description="Subregion should not be NULL",
    check_category="POST_JOIN",
    check_name="Mandatory Field Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=null_subregion,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# Verify Post-Join DQ Logs
spark.table(dq_logger_table) \
    .filter(
        (col("pipeline_run_id") == pipeline_run_id) &
        (col("check_category") == "POST_JOIN")
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

# MAGIC %md
# MAGIC #### Task 13 : Filter final records removing post-join DQ failures
# MAGIC

# COMMAND ----------

# ==========================================
# Task 13 : Filter Final Records
# ==========================================

from pyspark.sql.functions import col

city_final_df = (
    city_join_df
    .filter(col("state_province_name").isNotNull())
    .filter(col("country_name").isNotNull())
    .filter(col("continent").isNotNull())
    .filter(col("sales_territory").isNotNull())
    .filter(col("region").isNotNull())
    .filter(col("subregion").isNotNull())
)

print("Records Before Filtering :", city_join_df.count())
print("Valid Silver Records     :", city_final_df.count())
print("Rejected Records         :", city_join_df.count() - city_final_df.count())

# COMMAND ----------

# Verify Final Dataset
display(
    city_final_df.select(
        "cityid",
        "cityname",
        "state_province_name",
        "country_name",
        "continent",
        "sales_territory",
        "region",
        "subregion"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 14 : Align final dataset with Silver table schema: select required business columns, prefix IDs (e.g., wwi_customer_id), add metadata columns

# COMMAND ----------

# ==========================================
# Task 14 : Align with Silver Schema
# ==========================================

from pyspark.sql.functions import *

city_silver_df = (
    city_final_df.select(
        col("cityid").alias("wwi_city_id"),
        col("cityname").alias("city"),
        col("state_province_name").alias("state_province"),
        col("country_name").alias("country"),
        col("continent").alias("continent"),
        col("sales_territory").alias("sales_territory"),
        col("region").alias("region"),
        col("subregion").alias("subregion"),
        col("location").alias("location"),
        col("latestrecordedpopulation").alias("latest_recorded_population"),

        # Metadata Columns
        lit(business_month).alias("business_month"),
        current_timestamp().alias("created_timestamp"),
        current_timestamp().alias("last_updated_timestamp"),
        lit(pipeline_run_id).alias("pipeline_run_id")
    )
)

display(city_silver_df)

# COMMAND ----------

# View Schema
city_silver_df.printSchema()

# COMMAND ----------

spark.table(silver_city_table).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 15 : Delta MERGE into Silver table using business key: update matching records with latest values and last_updated_timestamp, insert new records with all columns

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Task 15 : Delta MERGE (SCD Type 1)

from delta.tables import DeltaTable

# Get Silver Table Reference
silver_delta_table = DeltaTable.forName(
    spark,
    silver_city_table
)

# MERGE (SCD Type 1)
(
    silver_delta_table.alias("target")
    .merge(
        city_silver_df.alias("source"),
        """
        target.wwi_city_id = source.wwi_city_id
        AND target.business_month = source.business_month
        """
    )

    # Update existing records
    .whenMatchedUpdate(
        set={
            "city": "source.city",
            "state_province": "source.state_province",
            "country": "source.country",
            "continent": "source.continent",
            "sales_territory": "source.sales_territory",
            "region": "source.region",
            "subregion": "source.subregion",
            "location": "source.location",
            "latest_recorded_population": "source.latest_recorded_population",
            "business_month": "source.business_month",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    # Insert new records
    .whenNotMatchedInsert(
        values={
            "wwi_city_id": "source.wwi_city_id",
            "city": "source.city",
            "state_province": "source.state_province",
            "country": "source.country",
            "continent": "source.continent",
            "sales_territory": "source.sales_territory",
            "region": "source.region",
            "subregion": "source.subregion",
            "location": "source.location",
            "latest_recorded_population": "source.latest_recorded_population",
            "business_month": "source.business_month",
            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    .execute()
)

# COMMAND ----------

print("Silver City Table Count :")
spark.table(silver_city_table).count()

# COMMAND ----------

# Display Silver Table
display(
    spark.table(silver_city_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 16 : Display execution summary: total Silver records, completion message, pipeline_run_id, business_month

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

total_silver_records = spark.table(silver_city_table).count()

print("=" * 60)
print("          CITY STAGING PIPELINE COMPLETED")
print("=" * 60)

print(f"Pipeline Run ID      : {pipeline_run_id}")
print(f"Business Month       : {business_month}")
print(f"Silver Table         : {silver_city_table}")
print(f"Total Silver Records : {total_silver_records}")

print("=" * 60)
print("Status               : SUCCESS")
print("Task                 : 02_City_Staging")
print("Delta MERGE          : Completed Successfully")
print("=" * 60)

# COMMAND ----------

from pyspark.sql import Row

summary_df = spark.createDataFrame([
    Row(
        pipeline_run_id=pipeline_run_id,
        business_month=business_month,
        silver_table=silver_city_table,
        total_records=total_silver_records,
        status="SUCCESS"
    )
])

display(summary_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC wwi_city_id,
# MAGIC count(*) AS duplicate
# MAGIC FROM wwi.silver.city_staging
# MAGIC GROUP BY wwi_city_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC