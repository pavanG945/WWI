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

dbutils.widgets.text(
    "business_month",
    "2016-06",
    "Business Month"
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
# MAGIC Task 2: Set up notebook: define Bronze source, Silver target table, DQ logger table, generate unique pipeline_run_id

# COMMAND ----------

import uuid

# Bronze tables
bronze_stockitem_table = "wwi.bronze.warehouse_stockitems"
bronze_color_table = "wwi.bronze.warehouse_colors"
bronze_packagetype_table = "wwi.bronze.warehouse_packagetypes"

# Silver tables
silver_stockitem_table = "wwi.silver.stockitem_staging"

# DQ Logger
dq_logger_table = "wwi.logger.dq_logger"

# Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("Pipeline Run ID:", pipeline_run_id)

# COMMAND ----------

# ==========================================
# Task 3 : Verify DQ Logger
# ==========================================

if spark.catalog.tableExists(dq_logger_table):
    print("DQ Logger table found.")
else:
    raise Exception(f"{dq_logger_table} not found.")

# COMMAND ----------

print(log_dq)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 5 : Load Bronze tables required for staging, filtered by business_month (June 2016)

# COMMAND ----------

# Task 5 : Load Bronze Tables
stockitem_df = (
    spark.table(bronze_stockitem_table)
    .filter(col("business_month") == business_month)
)

color_df = (
    spark.table(bronze_color_table)
    .filter(col("business_month") == business_month)
)

packagetype_df = (
    spark.table(bronze_packagetype_table)
    .filter(col("business_month") == business_month)
)

# Validate Input data
print("Stock Item Records :", stockitem_df.count())
print("Color Records      :", color_df.count())
print("Package Type Records :", packagetype_df.count())

if stockitem_df.count() == 0:
    raise Exception("No Stock Item records found.")

# COMMAND ----------

stockitem_df.printSchema()
color_df.printSchema()
packagetype_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 6 : Standardize raw Bronze data: cast ID columns to integer, validate email/phone with regex, trim text fields, apply title case to names, clean postal codes

# COMMAND ----------

# ==========================================
# Task 6 : Standardize Stock Item Data
# ==========================================

from pyspark.sql.functions import *

stockitem_df = (

    stockitem_df

    # =====================================
    # Cast ID Columns
    # =====================================
    .withColumn("stockitemid", col("stockitemid").cast("int"))
    .withColumn("supplierid", col("supplierid").cast("double").cast("int"))
    .withColumn("colorid", col("colorid").cast("double").cast("int"))
    .withColumn("unitpackageid", col("unitpackageid").cast("double").cast("int"))
    .withColumn("outerpackageid", col("outerpackageid").cast("double").cast("int"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    # =====================================
    # Cast Numeric Columns
    # =====================================
    .withColumn("unitprice", col("unitprice").cast("double"))
    .withColumn("taxrate", col("taxrate").cast("double"))
    .withColumn("recommendedretailprice", col("recommendedretailprice").cast("double"))
    .withColumn("typicalweightperunit", col("typicalweightperunit").cast("double"))
    .withColumn("quantityperouter", col("quantityperouter").cast("int"))
    .withColumn("leadtimedays", col("leadtimedays").cast("int"))

    # =====================================
    # Trim Text Columns
    # =====================================
    .withColumn("stockitemname", trim(col("stockitemname")))
    .withColumn("brand", trim(col("brand")))
    .withColumn("marketingcomments", trim(col("marketingcomments")))
    .withColumn("searchdetails", trim(col("searchdetails")))
    .withColumn("size", trim(col("size")))
    .withColumn("barcode", trim(col("barcode")))
    .withColumn("internalcomments", trim(col("internalcomments")))
    .withColumn("tags", trim(col("tags")))

    # =====================================
    # Title Case
    # =====================================
    .withColumn("stockitemname", initcap(col("stockitemname")))
    .withColumn("brand", initcap(col("brand")))

)

# COMMAND ----------

# ==========================================
# Standardize Color
# ==========================================

color_df = (

    color_df

    .withColumn("colorid", col("colorid").cast("double").cast("int"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    .withColumn("colorname", trim(col("colorname")))

    .withColumn("colorname", initcap(col("colorname")))

)

# COMMAND ----------

# ==========================================
# Standardize Package Type
# ==========================================

packagetype_df = (

    packagetype_df

    .withColumn("packagetypeid", col("packagetypeid").cast("double").cast("int"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    .withColumn("packagetypename", trim(col("packagetypename")))

    .withColumn("packagetypename", initcap(col("packagetypename")))

)

# COMMAND ----------

# ==========================================
# Verify Stock Item Standardization
# ==========================================

display(
    stockitem_df.select(
        "stockitemid",
        "stockitemname",
        "supplierid",
        "colorid",
        "unitpackageid",
        "outerpackageid",
        "brand",
        "quantityperouter",
        "unitprice",
        "taxrate",
        "leadtimedays"
    )
)

# COMMAND ----------

# ==========================================
# Verify Color Standardization
# ==========================================

display(
    color_df.select(
        "colorid",
        "colorname"
    )
)

# COMMAND ----------

# ==========================================
# Verify Package Type Standardization
# ==========================================

display(
    packagetype_df.select(
        "packagetypeid",
        "packagetypename"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 7 : Perform pre-join DQ checks: count total records, validate primary keys for NULLs and invalid values, validate required fields, check lookup table keys

# COMMAND ----------

# ==========================================
# Task 7.1 : Total Records
# ==========================================

total_records = stockitem_df.count()

print("Total Stock Item Records :", total_records)

# COMMAND ----------

# ==========================================
# Primary Key NULL Check - StockItemID
# ==========================================

null_stockitemid = stockitem_df.filter(
    col("stockitemid").isNull()
).count()

print("NULL Stock Item ID :", null_stockitemid)

log_dq(
    table_name="warehouse_stockitems",
    column_name="stockitemid",
    dq_type="NULL_CHECK",
    dq_description="Stock Item ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Primary Key NULL Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_stockitemid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Invalid Value Check - StockItemID
# ==========================================

invalid_stockitemid = stockitem_df.filter(
    col("stockitemid") <= 0
).count()

print("Invalid Stock Item ID :", invalid_stockitemid)

log_dq(
    table_name="warehouse_stockitems",
    column_name="stockitemid",
    dq_type="INVALID_VALUE",
    dq_description="Stock Item ID should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Primary Key Invalid Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=invalid_stockitemid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Duplicate Check - StockItemID
# ==========================================

duplicate_stockitemid = (
    stockitem_df
    .groupBy("stockitemid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Stock Item ID :", duplicate_stockitemid)

log_dq(
    table_name="warehouse_stockitems",
    column_name="stockitemid",
    dq_type="DUPLICATE_CHECK",
    dq_description="Stock Item ID should be unique",
    check_category="PRE_JOIN",
    check_name="Duplicate Primary Key Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=duplicate_stockitemid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Mandatory Field Check - Stock Item Name
# ==========================================

null_stockitemname = stockitem_df.filter(
    col("stockitemname").isNull() |
    (trim(col("stockitemname")) == "")
).count()

print("NULL Stock Item Name :", null_stockitemname)

log_dq(
    table_name="warehouse_stockitems",
    column_name="stockitemname",
    dq_type="MANDATORY_FIELD",
    dq_description="Stock Item Name should not be NULL",
    check_category="PRE_JOIN",
    check_name="Mandatory Field Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_stockitemname,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

stockitem_df.filter(
    col("colorid").isNull()
).count()

# COMMAND ----------

# ==========================================
# Lookup Check - ColorID
# ==========================================

invalid_colorid = (
    stockitem_df.alias("s")
    .join(
        color_df.alias("c"),
        col("s.colorid") == col("c.colorid"),
        "left"
    )
    .filter(col("c.colorid").isNull())
    .count()
)

print("Invalid Color ID :", invalid_colorid)

log_dq(
    table_name="warehouse_stockitems",
    column_name="colorid",
    dq_type="LOOKUP_KEY",
    dq_description="Color ID should exist in warehouse_colors",
    check_category="PRE_JOIN",
    check_name="Color Lookup Check",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_colorid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Lookup Check - Unit Package ID
# ==========================================

invalid_packageid = (
    stockitem_df.alias("s")
    .join(
        packagetype_df.alias("p"),
        col("s.unitpackageid") == col("p.packagetypeid"),
        "left"
    )
    .filter(col("p.packagetypeid").isNull())
    .count()
)

print("Invalid Package ID :", invalid_packageid)

log_dq(
    table_name="warehouse_stockitems",
    column_name="unitpackageid",
    dq_type="LOOKUP_KEY",
    dq_description="Unit Package ID should exist in warehouse_packagetypes",
    check_category="PRE_JOIN",
    check_name="Package Lookup Check",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_packageid,
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
# Task 9 : Filter Valid Stock Item Records
# ==========================================

stockitem_valid_df = (
    stockitem_df.alias("s")

    .join(
        color_df.alias("c"),
        col("s.colorid") == col("c.colorid"),
        "left"
    )

    .join(
        packagetype_df.alias("p"),
        col("s.unitpackageid") == col("p.packagetypeid"),
        "left"
    )

    .filter(col("s.stockitemid").isNotNull())
    .filter(col("s.stockitemid") > 0)
    .filter(col("s.stockitemname").isNotNull())
    .filter(trim(col("s.stockitemname")) != "")
    .filter(col("c.colorid").isNotNull())          # Remove invalid Color IDs
    .filter(col("p.packagetypeid").isNotNull())    # Remove invalid Package IDs

    .select("s.*")
)

# COMMAND ----------

print("Original Records :", stockitem_df.count())
print("Valid Records    :", stockitem_valid_df.count())
print("Rejected Records :", stockitem_df.count() - stockitem_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 10 : Prepare lookup tables: remove duplicates, ensure unique primary keys before joining

# COMMAND ----------

# ==========================================
# Task 10.1 : Prepare Color Lookup
# ==========================================

color_lookup_df = (
    color_df
    .dropDuplicates(["colorid"])
)

print("Original Color Records :", color_df.count())
print("Unique Color Records   :", color_lookup_df.count())

# COMMAND ----------

# ==========================================
# Task 10.2 : Prepare Package Type Lookup
# ==========================================

packagetype_lookup_df = (
    packagetype_df
    .dropDuplicates(["packagetypeid"])
)

print("Original Package Records :", packagetype_df.count())
print("Unique Package Records   :", packagetype_lookup_df.count())

# COMMAND ----------

display(color_lookup_df)
display(packagetype_lookup_df)
print("Color Lookup Count :", color_lookup_df.count())
print("Package Lookup Count :", packagetype_lookup_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Task 11 : Join main table with lookup tables using left joins to avoid data loss

# COMMAND ----------

from pyspark.sql.functions import col

# ==========================================
# Join Color Lookup
# ==========================================

stockitem_color_df = (
    stockitem_valid_df.alias("s")
    .join(
        color_lookup_df.alias("c"),
        col("s.colorid") == col("c.colorid"),
        "left"
    )
    .select(
        col("s.*"),
        col("c.colorname").alias("color_name")
    )
)

# COMMAND ----------

print("Records After Color Join :", stockitem_color_df.count())

display(
    stockitem_color_df.select(
        "stockitemid",
        "stockitemname",
        "colorid",
        "color_name"
    )
)

# COMMAND ----------

# ==========================================
# Join Unit Package Lookup
# ==========================================

stockitem_unitpackage_df = (
    stockitem_color_df.alias("s")
    .join(
        packagetype_lookup_df.alias("p"),
        col("s.unitpackageid") == col("p.packagetypeid"),
        "left"
    )
    .select(
        col("s.*"),
        col("p.packagetypename").alias("unit_package_name")
    )
)

# COMMAND ----------

print("Records After Unit Package Join :", stockitem_unitpackage_df.count())

display(
    stockitem_unitpackage_df.select(
        "stockitemid",
        "stockitemname",
        "unitpackageid",
        "unit_package_name"
    )
)

# COMMAND ----------

# ==========================================
# Join Outer Package Lookup
# ==========================================

stockitem_join_df = (
    stockitem_unitpackage_df.alias("s")
    .join(
        packagetype_lookup_df.alias("p"),
        col("s.outerpackageid") == col("p.packagetypeid"),
        "left"
    )
    .select(
        col("s.*"),
        col("p.packagetypename").alias("outer_package_name")
    )
)

# COMMAND ----------

print("Records After Outer Package Join :", stockitem_join_df.count())

display(
    stockitem_join_df.select(
        "stockitemid",
        "stockitemname",
        "color_name",
        "unit_package_name",
        "outer_package_name"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Task 12 : Perform post-join DQ checks: validate mandatory fields, referential integrity for lookup names, capture failed counts, log to DQ logger

# COMMAND ----------

# ==========================================
# Post-Join DQ Check - Color Name
# ==========================================

null_color_name = stockitem_join_df.filter(
    col("color_name").isNull()
).count()

print("NULL Color Name :", null_color_name)

log_dq(
    table_name="warehouse_stockitems",
    column_name="color_name",
    dq_type="POST_JOIN",
    dq_description="Color Name should exist after lookup join",
    check_category="POST_JOIN",
    check_name="Color Lookup Validation",
    severity="HIGH",
    total_records=stockitem_join_df.count(),
    failed_records=null_color_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Post-Join DQ Check - Unit Package Name
# ==========================================

null_unit_package = stockitem_join_df.filter(
    col("unit_package_name").isNull()
).count()

print("NULL Unit Package Name :", null_unit_package)

log_dq(
    table_name="warehouse_stockitems",
    column_name="unit_package_name",
    dq_type="POST_JOIN",
    dq_description="Unit Package Name should exist after lookup join",
    check_category="POST_JOIN",
    check_name="Unit Package Lookup Validation",
    severity="HIGH",
    total_records=stockitem_join_df.count(),
    failed_records=null_unit_package,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Post-Join DQ Check - Outer Package Name
# ==========================================

null_outer_package = stockitem_join_df.filter(
    col("outer_package_name").isNull()
).count()

print("NULL Outer Package Name :", null_outer_package)

log_dq(
    table_name="warehouse_stockitems",
    column_name="outer_package_name",
    dq_type="POST_JOIN",
    dq_description="Outer Package Name should exist after lookup join",
    check_category="POST_JOIN",
    check_name="Outer Package Lookup Validation",
    severity="HIGH",
    total_records=stockitem_join_df.count(),
    failed_records=null_outer_package,
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
    .orderBy("check_timestamp") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Task 13 : Filter final records removing post-join DQ failures

# COMMAND ----------

from pyspark.sql.functions import col, trim

# ==========================================
# Task 13 : Filter Final Valid Records
# ==========================================

stockitem_final_df = (
    stockitem_join_df

    .filter(col("color_name").isNotNull())
    .filter(trim(col("color_name")) != "")

    .filter(col("unit_package_name").isNotNull())
    .filter(trim(col("unit_package_name")) != "")

    .filter(col("outer_package_name").isNotNull())
    .filter(trim(col("outer_package_name")) != "")
)
print("Records After Join      :", stockitem_join_df.count())
print("Final Valid Records     :", stockitem_final_df.count())
print("Rejected After Post Join:", stockitem_join_df.count() - stockitem_final_df.count())

# COMMAND ----------

display(
    stockitem_final_df.select(
        "stockitemid",
        "stockitemname",
        "color_name",
        "unit_package_name",
        "outer_package_name"
    )
)

# COMMAND ----------

spark.table(silver_stockitem_table).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Task 14 : Align final dataset with Silver table schema: select required business columns, prefix IDs (e.g., wwi_customer_id), add metadata columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

stockitem_silver_df = stockitem_final_df.select(

    col("stockitemid").alias("stock_item_id"),
    col("stockitemname").alias("stock_item_name"),
    col("supplierid").alias("supplier_id"),
    col("colorid").alias("color_id"),
    col("color_name"),
    col("unitpackageid").alias("unit_package_id"),
    col("unit_package_name"),
    col("brand"),
    col("unitprice").alias("unit_price"),
    col("taxrate").alias("tax_rate"),
    col("leadtimedays").alias("lead_time_days"),

    current_timestamp().alias("created_timestamp"),
    current_timestamp().alias("last_updated_timestamp"),

    lit(pipeline_run_id).alias("pipeline_run_id"),
    lit(business_month).alias("business_month")
)

# COMMAND ----------

from pyspark.sql.functions import lit

stockitem_silver_df = stockitem_silver_df.withColumn(
    "pipeline_run_id",
    lit(pipeline_run_id)
)

# COMMAND ----------

stockitem_silver_df.printSchema()

display(stockitem_silver_df)

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
    silver_stockitem_table
)

# Perform SCD Type 1 MERGE
(
    silver_delta_table.alias("target")
    .merge(
        stockitem_silver_df.alias("source"),
        """
        target.stock_item_id = source.stock_item_id AND
        target.business_month = source.business_month
        """
    )

    # Update existing records
    .whenMatchedUpdate(
        set={
            "stock_item_name": "source.stock_item_name",
            "supplier_id": "source.supplier_id",
            "color_id": "source.color_id",
            "color_name": "source.color_name",
            "unit_package_id": "source.unit_package_id",
            "unit_package_name": "source.unit_package_name",
            "brand": "source.brand",
            "unit_price": "source.unit_price",
            "tax_rate": "source.tax_rate",
            "lead_time_days": "source.lead_time_days",
            "business_month": "source.business_month",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    # Insert new records
    .whenNotMatchedInsert(
        values={
            "stock_item_id": "source.stock_item_id",
            "stock_item_name": "source.stock_item_name",
            "supplier_id": "source.supplier_id",
            "color_id": "source.color_id",
            "color_name": "source.color_name",
            "unit_package_id": "source.unit_package_id",
            "unit_package_name": "source.unit_package_name",
            "brand": "source.brand",
            "unit_price": "source.unit_price",
            "tax_rate": "source.tax_rate",
            "lead_time_days": "source.lead_time_days",
            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id",
            "business_month": "source.business_month"
        }
    )

    .execute()
)

# COMMAND ----------

print("Silver Stock Item Table Count :")

print(
    spark.table(silver_stockitem_table).count()
)

display(
    spark.table(silver_stockitem_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 16 : Display execution summary: total Silver records, completion message, pipeline_run_id, business_month

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

silver_record_count = spark.table(silver_stockitem_table).count()

print("=" * 60)
print("Stock Item Staging Pipeline Completed Successfully")
print("=" * 60)

print(f"Business Month        : {business_month}")
print(f"Pipeline Run ID       : {pipeline_run_id}")
print(f"Total Silver Records  : {silver_record_count}")

print("=" * 60)
print("Data successfully loaded into:", silver_stockitem_table)
print("=" * 60)

# COMMAND ----------

display(
    spark.table(silver_stockitem_table)
)

# COMMAND ----------

