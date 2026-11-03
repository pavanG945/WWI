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

if business_month == "":
    raise Exception("Business Month is Mandatory")

print(f"Business Month : {business_month}")

# COMMAND ----------

# ==========================================
# Task 2 : Notebook Setup
# ==========================================

import uuid
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

# Main Bronze Table
bronze_purchase_line_table = "wwi.bronze.purchasing_purchaseorderlines"

# Lookup Tables
bronze_purchase_order_table = "wwi.bronze.purchasing_purchaseorders"
bronze_stockitem_table = "wwi.bronze.warehouse_stockitems"
bronze_packagetype_table = "wwi.bronze.warehouse_packagetypes"

# Silver Table
silver_purchase_table = "wwi.silver.purchase_staging"

# DQ Logger
dq_logger_table = "wwi.logger.dq_logger"

# Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("=" * 60)
print("Pipeline Run ID :", pipeline_run_id)
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 3 : Verify Central DQ Logger
# ==========================================

spark.table(dq_logger_table).printSchema()

display(
    spark.table(dq_logger_table).limit(5)
)

# COMMAND ----------

print(log_dq)

# COMMAND ----------

# ==========================================
# Task 5 : Load Bronze Tables
# ==========================================

purchase_line_df = (
    spark.table(bronze_purchase_line_table)
    .filter(col("business_month") == business_month)
)

purchase_order_df = (
    spark.table(bronze_purchase_order_table)
    .filter(col("business_month") == business_month)
)

stockitem_df = (
    spark.table(bronze_stockitem_table)
    .filter(col("business_month") == business_month)
)

packagetype_df = (
    spark.table(bronze_packagetype_table)
    .filter(col("business_month") == business_month)
)
print("Purchase Order Line Records :", purchase_line_df.count())
print("Purchase Order Records      :", purchase_order_df.count())
print("Stock Item Records          :", stockitem_df.count())
print("Package Type Records        :", packagetype_df.count())


if purchase_line_df.count() == 0:
    raise Exception(
        "No Purchase Order Line records found for selected Business Month."
    )

# COMMAND ----------

purchase_line_df.printSchema()
purchase_order_df.printSchema()
stockitem_df.printSchema()
packagetype_df.printSchema()

# COMMAND ----------



# COMMAND ----------

# ==========================================
# Task 6 : Standardize Purchase Data
# ==========================================

from pyspark.sql.functions import *

purchase_line_df = (

    purchase_line_df

    # =====================================
    # Cast ID Columns
    # =====================================

    .withColumn(
        "purchaseorderlineid",
        col("purchaseorderlineid").cast("long")
    )

    .withColumn(
        "purchaseorderid",
        col("purchaseorderid").cast("long")
    )

    .withColumn(
        "stockitemid",
        col("stockitemid").cast("long")
    )

    .withColumn(
        "packagetypeid",
        col("packagetypeid").cast("long")
    )

    .withColumn(
        "lasteditedby",
        col("lasteditedby").cast("int")
    )

    # =====================================
    # Numeric Columns
    # =====================================

    .withColumn(
        "orderedouters",
        col("orderedouters").cast("int")
    )

    .withColumn(
        "receivedouters",
        col("receivedouters").cast("int")
    )

    .withColumn(
        "expectedunitpriceperouter",
        col("expectedunitpriceperouter").cast("double")
    )

    # =====================================
    # Trim Text Columns
    # =====================================

    .withColumn(
        "description",
        trim(col("description"))
    )

    # =====================================
    # Boolean Column
    # =====================================

    .withColumn(
        "isorderlinefinalized",
        col("isorderlinefinalized").cast("boolean")
    )

    # =====================================
    # Timestamp Columns
    # =====================================

    .withColumn(
        "lasteditedwhen",
        to_timestamp(
            col("lasteditedwhen"),
            "dd-MM-yyyy HH:mm:ss"
        )
    )

    .withColumn(
        "lastreceiptdate",
        to_timestamp(
            col("lastreceiptdate"),
            "dd-MM-yyyy HH:mm:ss"
        )
    )

    .withColumn(
        "ingestion_timestamp",
        col("ingestion_timestamp").cast("timestamp")
    )

)

# COMMAND ----------

purchase_line_df.select(
    "purchaseorderlineid",
    "purchaseorderid",
    "stockitemid",
    "packagetypeid",
    "orderedouters",
    "receivedouters",
    "expectedunitpriceperouter",
    "isorderlinefinalized",
    "lasteditedwhen",
    "lastreceiptdate",
    "business_month"
).show(10, truncate=False)

# COMMAND ----------

# ==========================================
# Task 6B : Standardize Purchase Orders
# ==========================================

from pyspark.sql.functions import *

purchase_order_df = (

    purchase_order_df

    # Cast IDs
    .withColumn("purchaseorderid", col("purchaseorderid").cast("long"))
    .withColumn("supplierid", col("supplierid").cast("long"))
    .withColumn("contactpersonid", col("contactpersonid").cast("long"))
    .withColumn("deliverymethodid", col("deliverymethodid").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("int"))

    # Trim Text
    .withColumn("supplierreference", trim(col("supplierreference")))
    .withColumn("comments", trim(col("comments")))
    .withColumn("internalcomments", trim(col("internalcomments")))

    # Boolean
    .withColumn("isorderfinalized", col("isorderfinalized").cast("boolean"))

    # Dates
    .withColumn("orderdate",
        to_timestamp(col("orderdate"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn("expecteddeliverydate",
        to_timestamp(col("expecteddeliverydate"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn("lasteditedwhen",
        to_timestamp(col("lasteditedwhen"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn(
        "ingestion_timestamp",
        col("ingestion_timestamp").cast("timestamp")
    )

)

# COMMAND ----------

# ==========================================
# Task 6C : Standardize Stock Items
# ==========================================

stockitem_df = (

    stockitem_df

    # Cast IDs
    .withColumn("stockitemid", col("stockitemid").cast("long"))
    .withColumn("supplierid", col("supplierid").cast("long"))
    .withColumn("colorid", col("colorid").cast("long"))
    .withColumn("outerpackageid", col("outerpackageid").cast("long"))
    .withColumn("unitpackageid", col("unitpackageid").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("int"))

    # Numeric
    .withColumn("quantityperouter", col("quantityperouter").cast("int"))
    .withColumn("recommendedretailprice", col("recommendedretailprice").cast("double"))
    .withColumn("unitprice", col("unitprice").cast("double"))
    .withColumn("taxrate", col("taxrate").cast("double"))
    .withColumn("typicalweightperunit", col("typicalweightperunit").cast("double"))
    .withColumn("leadtimedays", col("leadtimedays").cast("int"))

    # Boolean
    .withColumn("ischillerstock", col("ischillerstock").cast("boolean"))

    # Trim Text
    .withColumn("stockitemname", initcap(trim(col("stockitemname"))))
    .withColumn("brand", trim(col("brand")))
    .withColumn("barcode", trim(col("barcode")))
    .withColumn("size", trim(col("size")))
    .withColumn("photo", trim(col("photo")))
    .withColumn("marketingcomments", trim(col("marketingcomments")))
    .withColumn("internalcomments", trim(col("internalcomments")))
    .withColumn("searchdetails", trim(col("searchdetails")))
    .withColumn("tags", trim(col("tags")))
    .withColumn("customfields", trim(col("customfields")))

    # Dates
    .withColumn("validfrom",
        to_timestamp(col("validfrom"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn("validto",
        to_timestamp(col("validto"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn(
        "ingestion_timestamp",
        col("ingestion_timestamp").cast("timestamp")
    )

)

# COMMAND ----------

# ==========================================
# Task 6D : Standardize Package Types
# ==========================================

packagetype_df = (

    packagetype_df

    # Cast IDs
    .withColumn("packagetypeid", col("packagetypeid").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("int"))

    # Trim Text
    .withColumn(
        "packagetypename",
        initcap(trim(col("packagetypename")))
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

    .withColumn(
        "ingestion_timestamp",
        col("ingestion_timestamp").cast("timestamp")
    )

)

# COMMAND ----------

purchase_line_df.printSchema()
purchase_order_df.printSchema()
stockitem_df.printSchema()
packagetype_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 7 : Pre-Join Data Quality Checks
# ==========================================

total_records = purchase_line_df.count()

print("=" * 60)
print("Total Purchase Order Line Records :", total_records)
print("=" * 60)

# COMMAND ----------

null_purchaseorderlineid = purchase_line_df.filter(
    col("purchaseorderlineid").isNull()
).count()

print("NULL Purchase Order Line ID :", null_purchaseorderlineid)

log_dq(
    table_name="purchasing_purchaseorderlines",
    column_name="purchaseorderlineid",
    dq_type="NULL_CHECK",
    dq_description="Purchase Order Line ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Primary Key NULL Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_purchaseorderlineid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

invalid_purchaseorderlineid = purchase_line_df.filter(
    col("purchaseorderlineid") <= 0
).count()

print("Invalid Purchase Order Line ID :", invalid_purchaseorderlineid)

log_dq(
    table_name="purchasing_purchaseorderlines",
    column_name="purchaseorderlineid",
    dq_type="INVALID_VALUE",
    dq_description="Purchase Order Line ID should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Primary Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=invalid_purchaseorderlineid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

duplicate_purchaseorderlineid = (
    purchase_line_df
    .groupBy("purchaseorderlineid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Purchase Order Line IDs :", duplicate_purchaseorderlineid)

log_dq(
    table_name="purchasing_purchaseorderlines",
    column_name="purchaseorderlineid",
    dq_type="DUPLICATE_CHECK",
    dq_description="Purchase Order Line ID should be unique",
    check_category="PRE_JOIN",
    check_name="Duplicate Primary Key",
    severity="HIGH",
    total_records=total_records,
    failed_records=duplicate_purchaseorderlineid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_purchaseorderid = purchase_line_df.filter(
    col("purchaseorderid").isNull()
).count()

print("NULL Purchase Order ID :", null_purchaseorderid)

log_dq(
    table_name="purchasing_purchaseorderlines",
    column_name="purchaseorderid",
    dq_type="MANDATORY_FIELD",
    dq_description="Purchase Order ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Purchase Order Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_purchaseorderid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_stockitemid = purchase_line_df.filter(
    col("stockitemid").isNull()
).count()

print("NULL Stock Item ID :", null_stockitemid)

log_dq(
    table_name="purchasing_purchaseorderlines",
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

null_packagetypeid = purchase_line_df.filter(
    col("packagetypeid").isNull()
).count()

print("NULL Package Type ID :", null_packagetypeid)

log_dq(
    table_name="purchasing_purchaseorderlines",
    column_name="packagetypeid",
    dq_type="MANDATORY_FIELD",
    dq_description="Package Type ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Package Type Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=null_packagetypeid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

invalid_orderedouters = purchase_line_df.filter(
    (col("orderedouters").isNull()) |
    (col("orderedouters") <= 0)
).count()

print("Invalid Ordered Outers :", invalid_orderedouters)

log_dq(
    table_name="purchasing_purchaseorderlines",
    column_name="orderedouters",
    dq_type="INVALID_VALUE",
    dq_description="Ordered Outers should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Ordered Outers Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_orderedouters,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

invalid_receivedouters = purchase_line_df.filter(
    col("receivedouters").isNull()
).count()

print("NULL Received Outers :", invalid_receivedouters)

log_dq(
    table_name="purchasing_purchaseorderlines",
    column_name="receivedouters",
    dq_type="MANDATORY_FIELD",
    dq_description="Received Outers should not be NULL",
    check_category="PRE_JOIN",
    check_name="Received Outers Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_receivedouters,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_lasteditedwhen = purchase_line_df.filter(
    col("lasteditedwhen").isNull()
).count()

print("NULL Last Edited When :", null_lasteditedwhen)

log_dq(
    table_name="purchasing_purchaseorderlines",
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
# Task 8 : Verify Pre-Join DQ Logs
# ==========================================

display(
    spark.table(dq_logger_table)
    .filter(col("pipeline_run_id") == pipeline_run_id)
    .filter(col("check_category") == "PRE_JOIN")
    .orderBy("check_timestamp")
)

# COMMAND ----------

spark.table(dq_logger_table) \
    .filter(col("pipeline_run_id") == pipeline_run_id) \
    .filter(col("check_category") == "PRE_JOIN") \
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
    .filter(col("check_category") == "PRE_JOIN")
    .count()
)

print("=" * 60)
print("Total Pre-Join DQ Checks Logged :", dq_check_count)
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 9 : Filter Valid Purchase Records
# ==========================================

from pyspark.sql.functions import col

purchase_valid_df = (
    purchase_line_df

    .filter(col("purchaseorderlineid").isNotNull())
    .filter(col("purchaseorderlineid") > 0)

    .filter(col("purchaseorderid").isNotNull())

    .filter(col("stockitemid").isNotNull())

    .filter(col("packagetypeid").isNotNull())

    .filter(col("orderedouters").isNotNull())
    .filter(col("orderedouters") > 0)

    .filter(col("receivedouters").isNotNull())

    .filter(col("lasteditedwhen").isNotNull())
)

print("Records Before Filtering :", purchase_line_df.count())
print("Valid Records            :", purchase_valid_df.count())
print("Rejected Records         :", purchase_line_df.count() - purchase_valid_df.count())

# COMMAND ----------

purchase_valid_df.printSchema()

# COMMAND ----------

purchase_valid_df.count()
purchase_valid_df.select(
    "purchaseorderlineid",
    "purchaseorderid",
    "stockitemid",
    "packagetypeid",
    "orderedouters",
    "receivedouters",
    "lasteditedwhen",
    "business_month"
).show(10, truncate=False)

# COMMAND ----------

# ==========================================
# Task 10A : Prepare Purchase Order Lookup
# ==========================================

purchase_order_lookup_df = (
    purchase_order_df
    .select(
        "purchaseorderid",
        "supplierid"
    )
    .dropDuplicates(["purchaseorderid"])
)

print("Purchase Order Lookup Records :", purchase_order_lookup_df.count())

# COMMAND ----------

purchase_order_lookup_df.printSchema()

display(purchase_order_lookup_df)

# COMMAND ----------

# ==========================================
# Task 10B : Prepare Stock Item Lookup
# ==========================================

stockitem_lookup_df = (
    stockitem_df
    .select(
        "stockitemid",
        "stockitemname"
    )
    .dropDuplicates(["stockitemid"])
)

print("Stock Item Lookup Records :", stockitem_lookup_df.count())

# COMMAND ----------

stockitem_lookup_df.printSchema()

display(stockitem_lookup_df)

# COMMAND ----------

# ==========================================
# Task 10C : Prepare Package Type Lookup
# ==========================================

packagetype_lookup_df = (
    packagetype_df
    .select(
        "packagetypeid",
        "packagetypename"
    )
    .dropDuplicates(["packagetypeid"])
)

print("Package Type Lookup Records :", packagetype_lookup_df.count())

# COMMAND ----------

packagetype_lookup_df.printSchema()

display(packagetype_lookup_df)

# COMMAND ----------

print("=" * 60)

print(
    "Purchase Order Duplicate Keys :",
    purchase_order_lookup_df.groupBy("purchaseorderid")
    .count()
    .filter("count > 1")
    .count()
)

print(
    "Stock Item Duplicate Keys :",
    stockitem_lookup_df.groupBy("stockitemid")
    .count()
    .filter("count > 1")
    .count()
)

print(
    "Package Type Duplicate Keys :",
    packagetype_lookup_df.groupBy("packagetypeid")
    .count()
    .filter("count > 1")
    .count()
)

print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 11 : Join Lookup Tables
# ==========================================

purchase_join_df = (

    purchase_valid_df.alias("pl")

    # Purchase Order Lookup
    .join(
        purchase_order_lookup_df.alias("po"),
        col("pl.purchaseorderid") == col("po.purchaseorderid"),
        "left"
    )

    # Stock Item Lookup
    .join(
        stockitem_lookup_df.alias("si"),
        col("pl.stockitemid") == col("si.stockitemid"),
        "left"
    )

    # Package Type Lookup
    .join(
        packagetype_lookup_df.alias("pt"),
        col("pl.packagetypeid") == col("pt.packagetypeid"),
        "left"
    )

    .select(

        col("pl.purchaseorderlineid"),
        col("pl.purchaseorderid"),
        col("pl.stockitemid"),
        col("pl.packagetypeid"),

        col("pl.description"),
        col("pl.expectedunitpriceperouter"),
        col("pl.orderedouters"),
        col("pl.receivedouters"),
        col("pl.isorderlinefinalized"),

        col("po.supplierid").alias("supplierid"),

        col("si.stockitemname").alias("stock_item"),

        col("pt.packagetypename").alias("package"),

        col("pl.lastreceiptdate"),
        col("pl.lasteditedwhen"),
        col("pl.lasteditedby"),
        col("pl.business_month"),
        col("pl.ingestion_timestamp")
    )

)

# COMMAND ----------

purchase_join_df.printSchema()

# COMMAND ----------

display(purchase_join_df)

# COMMAND ----------

print("Records After Join :", purchase_join_df.count())

# COMMAND ----------

purchase_join_df.select(
    "purchaseorderlineid",
    "supplierid",
    "stock_item",
    "package"
).show(20, truncate=False)

# COMMAND ----------

# ==========================================
# Task 12 : Post-Join Data Quality Checks
# ==========================================

post_total_records = purchase_join_df.count()

print("=" * 60)
print("Post Join Records :", post_total_records)
print("=" * 60)

# COMMAND ----------

null_supplierid = purchase_join_df.filter(
    col("supplierid").isNull()
).count()

print("NULL Supplier ID :", null_supplierid)

log_dq(
    table_name="purchase_staging",
    column_name="supplierid",
    dq_type="POST_VALIDATION",
    dq_description="Supplier ID should not be NULL after join",
    check_category="POST_JOIN",
    check_name="Post Validation - Supplier ID",
    severity="HIGH",
    total_records=post_total_records,
    failed_records=null_supplierid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_stockitem = purchase_join_df.filter(
    col("stock_item").isNull()
).count()

print("NULL Stock Item :", null_stockitem)

log_dq(
    table_name="purchase_staging",
    column_name="stock_item",
    dq_type="POST_VALIDATION",
    dq_description="Stock Item should not be NULL after join",
    check_category="POST_JOIN",
    check_name="Post Validation - Stock Item",
    severity="HIGH",
    total_records=post_total_records,
    failed_records=null_stockitem,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_package = purchase_join_df.filter(
    col("package").isNull()
).count()

print("NULL Package :", null_package)

log_dq(
    table_name="purchase_staging",
    column_name="package",
    dq_type="POST_VALIDATION",
    dq_description="Package should not be NULL after join",
    check_category="POST_JOIN",
    check_name="Post Validation - Package",
    severity="MEDIUM",
    total_records=post_total_records,
    failed_records=null_package,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

display(
    spark.table(dq_logger_table)
    .filter(col("pipeline_run_id") == pipeline_run_id)
    .filter(col("check_category") == "POST_JOIN")
)

# COMMAND ----------

# ==========================================
# Task 13 : Filter Final Records
# ==========================================

from pyspark.sql.functions import col

purchase_final_df = (
    purchase_join_df

    .filter(col("supplierid").isNotNull())
    .filter(col("stock_item").isNotNull())
    .filter(col("package").isNotNull())
)

print("Records Before Filtering :", purchase_join_df.count())
print("Valid Silver Records     :", purchase_final_df.count())
print("Rejected Records         :", purchase_join_df.count() - purchase_final_df.count())

# COMMAND ----------

# ==========================================
# Task 14 : Align Final Dataset with Silver Schema
# ==========================================

from pyspark.sql.functions import col, current_timestamp, lit

purchase_silver_df = (

    purchase_final_df

    .select(

        # Business Keys
        col("purchaseorderid").alias("wwi_purchase_order_id"),
        col("stockitemid").alias("wwi_stock_item_id"),

        # Business Columns
        col("orderedouters").alias("ordered_outers"),

        # Column Mapping Sheet
        col("orderedouters").alias("ordered_quantity"),

        col("receivedouters").alias("received_outers"),

        col("isorderlinefinalized").alias("is_order_line_finalized"),

        col("supplierid").alias("wwi_supplier_id"),

        col("package"),

        col("lastreceiptdate").alias("date_key"),

        col("lasteditedwhen").alias("last_modified_date"),

        col("business_month"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp"),

        col("lasteditedby").alias("last_edited_by"),

        lit(pipeline_run_id).alias("pipeline_run_id")

    )

)

# COMMAND ----------

purchase_silver_df.printSchema()

display(purchase_silver_df)

# COMMAND ----------

display(purchase_silver_df.select(
    "wwi_purchase_order_id",
    "wwi_stock_item_id",
    "ordered_outers",
    "ordered_quantity",
    "received_outers",
    "is_order_line_finalized",
    "wwi_supplier_id",
    "package",
    "date_key",
    "last_edited_by",
    "last_modified_date",
    "business_month"
).limit(10))

# COMMAND ----------

# DBTITLE 1,Task 15 : Delta MERGE (SCD Type 1) - Monthly Grain
# ==========================================
# Task 15 : Delta MERGE (SCD Type 1)
# ==========================================

from delta.tables import DeltaTable

# Get Silver Table Reference
silver_delta_table = DeltaTable.forName(
    spark,
    silver_purchase_table
)

# MERGE (SCD Type 1)
(
    silver_delta_table.alias("target")
    .merge(
        purchase_silver_df.alias("source"),
        "target.wwi_purchase_order_id = source.wwi_purchase_order_id "
        "AND target.wwi_stock_item_id = source.wwi_stock_item_id "
        "AND target.business_month = source.business_month"
    )

    # Update Existing Records
    .whenMatchedUpdate(
        set={
            "ordered_outers": "source.ordered_outers",
            "ordered_quantity": "source.ordered_quantity",
            "received_outers": "source.received_outers",
            "is_order_line_finalized": "source.is_order_line_finalized",
            "wwi_supplier_id": "source.wwi_supplier_id",
            "package": "source.package",
            "date_key": "source.date_key",
            "last_modified_date": "source.last_modified_date",
            "business_month": "source.business_month",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    # Insert New Records
    .whenNotMatchedInsert(
        values={
            "wwi_purchase_order_id": "source.wwi_purchase_order_id",
            "wwi_stock_item_id": "source.wwi_stock_item_id",
            "ordered_outers": "source.ordered_outers",
            "ordered_quantity": "source.ordered_quantity",
            "received_outers": "source.received_outers",
            "is_order_line_finalized": "source.is_order_line_finalized",
            "wwi_supplier_id": "source.wwi_supplier_id",
            "package": "source.package",
            "date_key": "source.date_key",
            "last_modified_date": "source.last_modified_date",
            "business_month": "source.business_month",
            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    .execute()
)

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

silver_record_count = spark.table(silver_purchase_table).count()

print("=" * 60)
print("Purchase Staging Pipeline Completed Successfully")
print("=" * 60)

print(f"Business Month        : {business_month}")
print(f"Pipeline Run ID       : {pipeline_run_id}")
print(f"Total Silver Records  : {silver_record_count}")

print("=" * 60)
print(f"Data successfully loaded into {silver_purchase_table}")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wwi.silver.purchase_staging
# MAGIC WHERE business_month = '2016-08'

# COMMAND ----------

