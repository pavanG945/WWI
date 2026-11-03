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

if business_month.strip() == "":
    raise ValueError("Business Month is required.")

print(f"Business Month : {business_month}")

# COMMAND ----------

# ==========================================
# Task 2 : Notebook Setup
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import uuid

# Bronze Tables
bronze_stockholding_table = "wwi.bronze.warehouse_stockitemholdings"
bronze_stockitem_table = "wwi.bronze.warehouse_stockitems"

# Silver Table
silver_stockholding_table = "wwi.silver.stockholding_staging"

# DQ Logger Table
dq_logger_table = "wwi.logger.dq_logger"

# Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("=" * 60)
print("Stock Holding Staging Pipeline Started")
print("=" * 60)
print(f"Business Month : {business_month}")
print(f"Pipeline Run ID: {pipeline_run_id}")

# COMMAND ----------

# ==========================================
# Task 3 : Verify DQ Logger Table
# ==========================================

spark.table(dq_logger_table)

print("DQ Logger Table Verified Successfully")

# COMMAND ----------

print(log_dq)

# COMMAND ----------

# ==========================================
# Task 5 : Load Bronze Tables
# ==========================================

stockholding_df = (

    spark.table(bronze_stockholding_table)

    .filter(
        col("business_month") == business_month
    )

)

stockitem_df = (

    spark.table(bronze_stockitem_table)

    .filter(
        col("business_month") == business_month
    )

)

print("Stock Holding Records :", stockholding_df.count())
print("Stock Item Records    :", stockitem_df.count())

# COMMAND ----------

stockholding_df.printSchema()
stockitem_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 6a : Standardize Stock Holding Table
# ==========================================

stockholding_df = (

    stockholding_df

    .withColumn("stockitemid", col("stockitemid").cast("double").cast("long"))
    .withColumn("quantityonhand", col("quantityonhand").cast("double").cast("integer"))
    .withColumn("laststocktakequantity", col("laststocktakequantity").cast("double").cast("integer"))
    .withColumn("reorderlevel", col("reorderlevel").cast("double").cast("integer"))
    .withColumn("targetstocklevel", col("targetstocklevel").cast("double").cast("integer"))

    .withColumn("lastcostprice", col("lastcostprice").cast("double"))

    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("integer"))

    .withColumn("lasteditedwhen", to_timestamp( col("lasteditedwhen"), "dd-MM-yyyy HH:mm"))

    .withColumn("binlocation", trim(col("binlocation")))

)

print("Stock Holding Standardized")

# COMMAND ----------

# ==========================================
# Task 6b : Standardize Stock Item Table
# ==========================================

stockitem_df = (

    stockitem_df

    .withColumn("stockitemid", col("stockitemid").cast("double").cast("long"))
    .withColumn("supplierid", col("supplierid").cast("double").cast("long"))
    .withColumn("colorid", col("colorid").cast("double").cast("long"))
    .withColumn("outerpackageid", col("outerpackageid").cast("double").cast("long"))
    .withColumn("unitpackageid", col("unitpackageid").cast("double").cast("long"))

    .withColumn("quantityperouter", col("quantityperouter").cast("double").cast("integer"))
    .withColumn("leadtimedays", col("leadtimedays").cast("double").cast("integer"))

    .withColumn("recommendedretailprice", col("recommendedretailprice").cast("double"))
    .withColumn("taxrate", col("taxrate").cast("double"))
    .withColumn("typicalweightperunit", col("typicalweightperunit").cast("double"))
    .withColumn("unitprice", col("unitprice").cast("double"))

    .withColumn("ischillerstock", col("ischillerstock").cast("boolean"))

    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("integer"))

    .withColumn("validfrom", to_timestamp(col("validfrom"), "dd-MM-yyyy HH:mm"))
    .withColumn("validto", to_timestamp(col("validto"), "dd-MM-yyyy HH:mm"))

    .withColumn("stockitemname", initcap(trim(col("stockitemname"))))
    .withColumn("brand", initcap(trim(col("brand"))))
    .withColumn("size", trim(col("size")))

)

print("Stock Item Standardized")

# COMMAND ----------

stockholding_df.printSchema()
stockitem_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 7 : Pre-Join DQ Checks
# ==========================================

total_records = stockholding_df.count()

# Primary Key
missing_stockitemid = stockholding_df.filter(
    col("stockitemid").isNull()
).count()

# Quantity On Hand
missing_quantity = stockholding_df.filter(
    col("quantityonhand").isNull()
).count()

invalid_quantity = stockholding_df.filter(
    col("quantityonhand") < 0
).count()

# Reorder Level
missing_reorder = stockholding_df.filter(
    col("reorderlevel").isNull()
).count()

invalid_reorder = stockholding_df.filter(
    col("reorderlevel") < 0
).count()

# Target Stock Level
missing_target = stockholding_df.filter(
    col("targetstocklevel").isNull()
).count()

invalid_target = stockholding_df.filter(
    col("targetstocklevel") < 0
).count()

# Last Cost Price
missing_cost = stockholding_df.filter(
    col("lastcostprice").isNull()
).count()

invalid_cost = stockholding_df.filter(
    col("lastcostprice") < 0
).count()

print("="*60)
print("Pre Join DQ Summary")
print("="*60)

print("Total Records           :", total_records)
print("Missing Stock Item ID   :", missing_stockitemid)
print("Missing Quantity        :", missing_quantity)
print("Invalid Quantity        :", invalid_quantity)
print("Missing Reorder Level   :", missing_reorder)
print("Invalid Reorder Level   :", invalid_reorder)
print("Missing Target Level    :", missing_target)
print("Invalid Target Level    :", invalid_target)
print("Missing Cost Price      :", missing_cost)
print("Invalid Cost Price      :", invalid_cost)

# COMMAND ----------

log_dq(
    table_name="stockholding_staging",
    column_name="stockitemid",
    dq_type="NULL_CHECK",
    dq_description="Stock Item ID cannot be NULL",
    check_category="PRE_JOIN",
    check_name="Stock Item ID Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_stockitemid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="stockholding_staging",
    column_name="quantityonhand",
    dq_type="INVALID_VALUE",
    dq_description="Quantity On Hand should be >= 0",
    check_category="PRE_JOIN",
    check_name="Quantity Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=(missing_quantity + invalid_quantity),
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="stockholding_staging",
    column_name="reorderlevel",
    dq_type="INVALID_VALUE",
    dq_description="Reorder Level should be >= 0",
    check_category="PRE_JOIN",
    check_name="Reorder Level Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=(missing_reorder + invalid_reorder),
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="stockholding_staging",
    column_name="targetstocklevel",
    dq_type="INVALID_VALUE",
    dq_description="Target Stock Level should be >= 0",
    check_category="PRE_JOIN",
    check_name="Target Stock Level Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=(missing_target + invalid_target),
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="stockholding_staging",
    column_name="lastcostprice",
    dq_type="INVALID_VALUE",
    dq_description="Last Cost Price should be >= 0",
    check_category="PRE_JOIN",
    check_name="Cost Price Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=(missing_cost + invalid_cost),
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

print("Pre-Join DQ Checks Logged Successfully")

# COMMAND ----------

# ==========================================
# Task 8 : Filter Valid Records
# ==========================================

stockholding_valid_df = (

    stockholding_df

    .filter(col("stockitemid").isNotNull())
    .filter(col("quantityonhand").isNotNull())
    .filter(col("quantityonhand") >= 0)

    .filter(col("reorderlevel").isNotNull())
    .filter(col("reorderlevel") >= 0)

    .filter(col("targetstocklevel").isNotNull())
    .filter(col("targetstocklevel") >= 0)

    .filter(col("lastcostprice").isNotNull())
    .filter(col("lastcostprice") >= 0)

)

print("Original Records :", stockholding_df.count())
print("Valid Records    :", stockholding_valid_df.count())
print("Rejected Records :", stockholding_df.count() - stockholding_valid_df.count())

# COMMAND ----------

# ==========================================
# Task 9 : Prepare Lookup Table
# ==========================================

stockitem_lookup_df = (

    stockitem_df

    .select(
        "stockitemid",
        "stockitemname"
    )

    .dropDuplicates(["stockitemid"])

)

print("Lookup Prepared Successfully")

# COMMAND ----------

# ==========================================
# Task 10 : Join Lookup Table
# ==========================================

stockholding_join_df = (

    stockholding_valid_df.alias("sh")

    .join(
        stockitem_lookup_df.alias("si"),
        col("sh.stockitemid") == col("si.stockitemid"),
        "left"
    )

    .select(

        col("sh.*"),

        col("si.stockitemname").alias("stock_item")

    )

)

print("Join Completed Successfully")

# COMMAND ----------

# ==========================================
# Task 11 : Post-Join DQ Checks
# ==========================================

total_records = stockholding_join_df.count()

missing_stock_item = stockholding_join_df.filter(
    col("stock_item").isNull()
).count()

print("=" * 60)
print("Post Join DQ Summary")
print("=" * 60)

print("Total Records      :", total_records)
print("Missing Stock Item :", missing_stock_item)

# COMMAND ----------

log_dq(
    table_name="stockholding_staging",
    column_name="stock_item",
    dq_type="MISSING_LOOKUP",
    dq_description="Stock Item not found in Stock Item lookup",
    check_category="POST_JOIN",
    check_name="Stock Item Lookup Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_stock_item,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

print("Post-Join DQ Checks Logged Successfully")

# COMMAND ----------

stockholding_join_df.filter(
    col("stock_item").isNull()
).select(
    "stockitemid",
    "quantityonhand",
    "business_month"
).show(truncate=False)

# COMMAND ----------

# ==========================================
# Task 12 : Filter Final Records
# ==========================================

stockholding_final_df = (

    stockholding_join_df

    .filter(col("stock_item").isNotNull())

)

print("=" * 60)
print("Post-Join Filtering Summary")
print("=" * 60)

print("Records Before Filtering :", stockholding_join_df.count())
print("Valid Silver Records     :", stockholding_final_df.count())
print("Rejected Records         :", stockholding_join_df.count() - stockholding_final_df.count())

# COMMAND ----------


stockholding_final_df.printSchema()

display(stockholding_final_df)

# COMMAND ----------

spark.table("wwi.silver.stockholding_staging").printSchema()

# COMMAND ----------

# ==========================================
# Task 13 : Create Silver DataFrame
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

stockholding_silver_df = (

    stockholding_final_df

    .select(

        # Business Keys
        col("stockitemid").alias("wwi_stock_item_id"),

        # Business Columns
        col("quantityonhand").alias("quantity_on_hand"),
        col("binlocation").alias("bin_location"),
        col("laststocktakequantity").alias("last_stocktake_quantity"),

        col("lastcostprice")
            .cast(DecimalType(10,0))
            .alias("last_cost_price"),

        col("reorderlevel").alias("reorder_level"),
        col("targetstocklevel").alias("target_stock_level"),

        col("stock_item").alias("stock_item_name"),

        col("lasteditedwhen").alias("last_modified_date"),

        # Metadata
        lit("WWI").alias("source_system"),

        current_timestamp().alias("created_timestamp"),
        current_timestamp().alias("updated_timestamp"),

        col("business_month"),

        lit(pipeline_run_id).alias("pipeline_run_id")

    )

)

print("=" * 60)
print("Stock Holding Silver DataFrame Created Successfully")
print("=" * 60)

# COMMAND ----------

stockholding_silver_df.printSchema()

display(stockholding_silver_df)

# COMMAND ----------

# ==========================================
# Task 14 : Merge into Silver Table
# ==========================================

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(
    spark,
    silver_stockholding_table
)

(
    silver_table.alias("target")

    .merge(
        stockholding_silver_df.alias("source"),
        """
        target.wwi_stock_item_id = source.wwi_stock_item_id
        AND target.business_month = source.business_month
        """
    )

    .whenMatchedUpdate(
        set={

            "quantity_on_hand": "source.quantity_on_hand",
            "bin_location": "source.bin_location",
            "last_stocktake_quantity": "source.last_stocktake_quantity",
            "last_cost_price": "source.last_cost_price",
            "reorder_level": "source.reorder_level",
            "target_stock_level": "source.target_stock_level",
            "stock_item_name": "source.stock_item_name",
            "last_modified_date": "source.last_modified_date",
            "source_system": "source.source_system",
            "updated_timestamp": "source.updated_timestamp",
            "business_month": "source.business_month",
            "pipeline_run_id": "source.pipeline_run_id"

        }
    )

    .whenNotMatchedInsert(
        values={

            "wwi_stock_item_id": "source.wwi_stock_item_id",
            "quantity_on_hand": "source.quantity_on_hand",
            "bin_location": "source.bin_location",
            "last_stocktake_quantity": "source.last_stocktake_quantity",
            "last_cost_price": "source.last_cost_price",
            "reorder_level": "source.reorder_level",
            "target_stock_level": "source.target_stock_level",
            "stock_item_name": "source.stock_item_name",
            "last_modified_date": "source.last_modified_date",
            "source_system": "source.source_system",
            "created_timestamp": "source.created_timestamp",
            "updated_timestamp": "source.updated_timestamp",
            "business_month": "source.business_month",
            "pipeline_run_id": "source.pipeline_run_id"

        }
    )

    .execute()
)

print("=" * 60)
print("Stock Holding Staging Merge Completed Successfully")
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 15 : Verify Silver Table
# ==========================================

# Verify Silver DataFrame Schema
stockholding_silver_df.printSchema()

# Display Silver DataFrame
display(stockholding_silver_df)

print("=" * 60)
print("Stock Holding Silver DataFrame Verified Successfully")
print("=" * 60)

# COMMAND ----------

display(
    spark.table(silver_stockholding_table)
    .filter(col("business_month") == business_month)
)

# COMMAND ----------

silver_count = (
    spark.table(silver_stockholding_table)
    .filter(col("business_month") == business_month)
    .count()
)

print(f"Total Silver Records : {silver_count}")

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

silver_record_count = (
    spark.table(silver_stockholding_table)
    .count()
)

print("=" * 60)
print("Stock Holding Staging Pipeline Completed Successfully")
print("=" * 60)

print(f"Business Month        : {business_month}")
print(f"Pipeline Run ID       : {pipeline_run_id}")
print(f"Total Silver Records  : {silver_record_count}")

print("=" * 60)
print(f"Data successfully loaded into {silver_stockholding_table}")
print("=" * 60)

# COMMAND ----------

