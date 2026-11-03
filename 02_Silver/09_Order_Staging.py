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

business_month = dbutils.widgets.get("business_month")

if business_month.strip() == "":
    raise Exception("Business Month is required.")

print("Business Month :", business_month)

# COMMAND ----------

# ==========================================
# Task 2 : Notebook Setup
# ==========================================

import uuid

# Bronze Tables
bronze_orders_table = "wwi.bronze.sales_orders"
bronze_orderlines_table = "wwi.bronze.sales_orderlines"
bronze_customers_table = "wwi.bronze.sales_customers"
bronze_cities_table = "wwi.bronze.application_cities"
bronze_people_table = "wwi.bronze.application_people"
bronze_stockitems_table = "wwi.bronze.warehouse_stockitems"
bronze_packagetypes_table = "wwi.bronze.warehouse_packagetypes"

# Silver Table
silver_order_table = "wwi.silver.order_staging"

# DQ Logger
dq_logger_table = "wwi.logger.dq_logger"

# Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("=" * 60)
print("Pipeline Run ID :", pipeline_run_id)
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 3 : Verify DQ Logger
# ==========================================

spark.sql(f"DESCRIBE TABLE {dq_logger_table}").show(truncate=False)

# COMMAND ----------

# ==========================================
# Task 5 : Load Bronze Tables
# ==========================================

orders_df = (
    spark.table(bronze_orders_table)
    .filter(col("business_month") == business_month)
)

orderline_df = (
    spark.table(bronze_orderlines_table)
    .filter(col("business_month") == business_month)
)

customer_df = (
    spark.table(bronze_customers_table)
    .filter(col("business_month") == business_month)
)

city_df = (
    spark.table(bronze_cities_table)
    .filter(col("business_month") == business_month)
)

people_df = (
    spark.table(bronze_people_table)
    .filter(col("business_month") == business_month)
)

stockitem_df = (
    spark.table(bronze_stockitems_table)
    .filter(col("business_month") == business_month)
)

packagetype_df = (
    spark.table(bronze_packagetypes_table)
    .filter(col("business_month") == business_month)
)

print("=" * 60)
print("Orders            :", orders_df.count())
print("Order Lines       :", orderline_df.count())
print("Customers         :", customer_df.count())
print("Cities            :", city_df.count())
print("People            :", people_df.count())
print("Stock Items       :", stockitem_df.count())
print("Package Types     :", packagetype_df.count())
print("=" * 60)

# COMMAND ----------

orders_df.printSchema()
orderline_df.printSchema()
customer_df.printSchema()
city_df.printSchema()
people_df.printSchema()
stockitem_df.printSchema()
packagetype_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Task 6A : Standardize Orders
# ==========================================
# Task 6A : Standardize Orders
# ==========================================

from pyspark.sql.functions import *

orders_df = (

    orders_df

    # ==========================
    # Cast ID Columns
    # ==========================
    .withColumn("orderid", col("orderid").cast("double").cast("long"))
    .withColumn("customerid", col("customerid").cast("double").cast("long"))
    .withColumn("contactpersonid", col("contactpersonid").cast("double").cast("long"))
    .withColumn("salespersonpersonid", col("salespersonpersonid").cast("double").cast("long"))
    .withColumn("pickedbypersonid", col("pickedbypersonid").cast("double").cast("long"))
    .withColumn("backorderorderid", col("backorderorderid").cast("double").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    # ==========================
    # Trim Text Columns
    # ==========================
    .withColumn("comments", trim(col("comments")))
    .withColumn("customerpurchaseordernumber", trim(col("customerpurchaseordernumber")))
    .withColumn("deliveryinstructions", trim(col("deliveryinstructions")))
    .withColumn("internalcomments", trim(col("internalcomments")))

    # ==========================
    # Boolean
    # ==========================
    .withColumn("isundersupplybackordered",
                col("isundersupplybackordered").cast("boolean"))

    # ==========================
    # Timestamp Columns
    # ==========================
    .withColumn("orderdate",
                to_timestamp(col("orderdate"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn("expecteddeliverydate",
                to_timestamp(col("expecteddeliverydate"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn("pickingcompletedwhen",
                to_timestamp(col("pickingcompletedwhen"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn("lasteditedwhen",
                to_timestamp(col("lasteditedwhen"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn(
        "ingestion_timestamp",
        col("ingestion_timestamp").cast("timestamp")
    )

)

# COMMAND ----------

# ==========================================
# Task 6B : Standardize Order Lines
# ==========================================

from pyspark.sql.functions import *

orderline_df = (

    orderline_df

    # ==========================
    # Cast ID Columns
    # ==========================
    .withColumn("orderlineid", col("orderlineid").cast("double").cast("long"))
    .withColumn("orderid", col("orderid").cast("double").cast("long"))
    .withColumn("stockitemid", col("stockitemid").cast("double").cast("long"))
    .withColumn("packagetypeid", col("packagetypeid").cast("double").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    # ==========================
    # Numeric Columns
    # ==========================
    .withColumn("quantity", col("quantity").cast("double").cast("int"))
    .withColumn("pickedquantity", col("pickedquantity").cast("double").cast("int"))
    .withColumn("unitprice", col("unitprice").cast("double").cast("double"))
    .withColumn("taxrate", col("taxrate").cast("double").cast("double"))

    # ==========================
    # Trim Text
    # ==========================
    .withColumn("description", trim(col("description")))

    # ==========================
    # Timestamp Columns
    # ==========================
    .withColumn(
        "lasteditedwhen",
        to_timestamp(col("lasteditedwhen"), "dd-MM-yyyy HH:mm:ss")
    )

    .withColumn(
        "pickingcompletedwhen",
        to_timestamp(col("pickingcompletedwhen"), "dd-MM-yyyy HH:mm:ss")
    )

    .withColumn(
        "ingestion_timestamp",
        col("ingestion_timestamp").cast("timestamp")
    )

)

# COMMAND ----------

# ==========================================
# Task 6C : Standardize Customers
# ==========================================

customer_df = (

    customer_df

    # Cast IDs
    .withColumn("customerid", col("customerid").cast("double").cast("long"))
    .withColumn("billtocustomerid", col("billtocustomerid").cast("double").cast("long"))
    .withColumn("buyinggroupid", col("buyinggroupid").cast("double").cast("long"))
    .withColumn("customercategoryid", col("customercategoryid").cast("double").cast("long"))
    .withColumn("deliverycityid", col("deliverycityid").cast("double").cast("long"))
    .withColumn("postalcityid", col("postalcityid").cast("double").cast("long"))
    .withColumn("deliverymethodid", col("deliverymethodid").cast("double").cast("long"))
    .withColumn("primarycontactpersonid", col("primarycontactpersonid").cast("double").cast("long"))
    .withColumn("alternatecontactpersonid", col("alternatecontactpersonid").cast("double").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    # Numeric
    .withColumn("creditlimit", col("creditlimit").cast("double"))
    .withColumn("paymentdays", col("paymentdays").cast("double").cast("int"))
    .withColumn("runposition", col("runposition").cast("double").cast("int"))
    .withColumn("standarddiscountpercentage", col("standarddiscountpercentage").cast("double").cast("double"))

    # Boolean
    .withColumn("isoncredithold", col("isoncredithold").cast("double").cast("boolean"))
    .withColumn("isstatementsent", col("isstatementsent").cast("double").cast("boolean"))

    # Trim Text
    .withColumn("customername", initcap(trim(col("customername"))))
    .withColumn("phonenumber", trim(col("phonenumber")))
    .withColumn("faxnumber", trim(col("faxnumber")))
    .withColumn("websiteurl", trim(col("websiteurl")))
    .withColumn("deliverypostalcode", regexp_replace(trim(col("deliverypostalcode")), "[^A-Za-z0-9]", ""))
    .withColumn("postalpostalcode", regexp_replace(trim(col("postalpostalcode")), "[^A-Za-z0-9]", ""))

    # Dates
    .withColumn("accountopeneddate", to_timestamp(col("accountopeneddate"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("validfrom", to_timestamp(col("validfrom"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("validto", to_timestamp(col("validto"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))

)

# COMMAND ----------

# ==========================================
# Task 6D : Standardize Cities
# ==========================================

city_df = (

    city_df

    .withColumn("cityid", col("cityid").cast("double").cast("long"))
    .withColumn("stateprovinceid", col("stateprovinceid").cast("double").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))
    .withColumn("latestrecordedpopulation", col("latestrecordedpopulation").cast("double").cast("long"))

    .withColumn("cityname", initcap(trim(col("cityname"))))
    .withColumn("location", trim(col("location")))

    .withColumn("validfrom", to_timestamp(col("validfrom"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("validto", to_timestamp(col("validto"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))

)

# COMMAND ----------

# ==========================================
# Task 6E : Standardize People
# ==========================================

people_df = (

    people_df

    .withColumn("personid", col("personid").cast("double").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    .withColumn("isemployee", col("isemployee").cast("double").cast("boolean"))
    .withColumn("issalesperson", col("issalesperson").cast("double").cast("boolean"))
    .withColumn("issystemuser", col("issystemuser").cast("double").cast("boolean"))
    .withColumn("ispermittedtologon", col("ispermittedtologon").cast("double").cast("boolean"))
    .withColumn("isexternallogonprovider", col("isexternallogonprovider").cast("double").cast("boolean"))

    .withColumn("fullname", initcap(trim(col("fullname"))))
    .withColumn("preferredname", initcap(trim(col("preferredname"))))
    .withColumn("emailaddress", lower(trim(col("emailaddress"))))
    .withColumn("phonenumber", trim(col("phonenumber")))

    .withColumn("validfrom", to_timestamp(col("validfrom"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("validto", to_timestamp(col("validto"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))

)

# COMMAND ----------

# ==========================================
# Task 6F : Standardize Stock Items
# ==========================================

from pyspark.sql.functions import *

stockitem_df = (

    stockitem_df

    # ==========================
    # Cast ID Columns
    # ==========================
    .withColumn("stockitemid", col("stockitemid").cast("double").cast("long"))
    .withColumn("supplierid", col("supplierid").cast("double").cast("long"))
    .withColumn("colorid", col("colorid").cast("double").cast("long"))
    .withColumn("outerpackageid", col("outerpackageid").cast("double").cast("long"))
    .withColumn("unitpackageid", col("unitpackageid").cast("double").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    # ==========================
    # Numeric Columns
    # ==========================
    .withColumn("quantityperouter", col("quantityperouter").cast("double").cast("int"))
    .withColumn("recommendedretailprice", col("recommendedretailprice").cast("double"))
    .withColumn("unitprice", col("unitprice").cast("double"))
    .withColumn("taxrate", col("taxrate").cast("double"))
    .withColumn("typicalweightperunit", col("typicalweightperunit").cast("double"))
    .withColumn("leadtimedays", col("leadtimedays").cast("double").cast("int"))

    # ==========================
    # Boolean
    # ==========================
    .withColumn("ischillerstock", col("ischillerstock").cast("double").cast("boolean"))

    # ==========================
    # Trim & Standardize Text
    # ==========================
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

    # ==========================
    # Date Columns
    # ==========================
    .withColumn("validfrom", to_timestamp(col("validfrom"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("validto", to_timestamp(col("validto"), "dd-MM-yyyy HH:mm:ss"))

    .withColumn(
        "ingestion_timestamp",
        col("ingestion_timestamp").cast("timestamp")
    )

)

# COMMAND ----------

# ==========================================
# Task 6G : Standardize Package Types
# ==========================================

from pyspark.sql.functions import *

packagetype_df = (

    packagetype_df

    # ==========================
    # Cast ID Columns
    # ==========================
    .withColumn("packagetypeid", col("packagetypeid").cast("double").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("int"))

    # ==========================
    # Trim & Title Case
    # ==========================
    .withColumn(
        "packagetypename",
        initcap(trim(col("packagetypename")))
    )

    # ==========================
    # Date Columns
    # ==========================
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

orders_df.printSchema()
orderline_df.printSchema()
customer_df.printSchema()
city_df.printSchema()
people_df.printSchema()
stockitem_df.printSchema()
packagetype_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 7 : Pre-Join Data Quality Checks
# ==========================================

total_records = orderline_df.count()

print("=" * 60)
print("Total Order Line Records :", total_records)
print("=" * 60)

# COMMAND ----------

null_orderlineid = orderline_df.filter(
    col("orderlineid").isNull()
).count()

print("NULL Order Line ID :", null_orderlineid)

log_dq(
    table_name="sales_orderlines",
    column_name="orderlineid",
    dq_type="NULL_CHECK",
    dq_description="Order Line ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Primary Key NULL Check",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_orderlineid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

invalid_orderlineid = orderline_df.filter(
    col("orderlineid") <= 0
).count()

print("Invalid Order Line ID :", invalid_orderlineid)

log_dq(
    table_name="sales_orderlines",
    column_name="orderlineid",
    dq_type="INVALID_VALUE",
    dq_description="Order Line ID should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Primary Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=invalid_orderlineid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

duplicate_orderlineid = (
    orderline_df
    .groupBy("orderlineid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Order Line IDs :", duplicate_orderlineid)

log_dq(
    table_name="sales_orderlines",
    column_name="orderlineid",
    dq_type="DUPLICATE_CHECK",
    dq_description="Order Line ID should be unique",
    check_category="PRE_JOIN",
    check_name="Duplicate Primary Key",
    severity="HIGH",
    total_records=total_records,
    failed_records=duplicate_orderlineid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_orderid = orderline_df.filter(
    col("orderid").isNull()
).count()

print("NULL Order ID :", null_orderid)

log_dq(
    table_name="sales_orderlines",
    column_name="orderid",
    dq_type="MANDATORY_FIELD",
    dq_description="Order ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Order Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=null_orderid,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_stockitemid = orderline_df.filter(
    col("stockitemid").isNull()
).count()

print("NULL Stock Item ID :", null_stockitemid)

log_dq(
    table_name="sales_orderlines",
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

null_packagetypeid = orderline_df.filter(
    col("packagetypeid").isNull()
).count()

print("NULL Package Type ID :", null_packagetypeid)

log_dq(
    table_name="sales_orderlines",
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

invalid_quantity = orderline_df.filter(
    (col("quantity").isNull()) |
    (col("quantity") <= 0)
).count()

print("Invalid Quantity :", invalid_quantity)

log_dq(
    table_name="sales_orderlines",
    column_name="quantity",
    dq_type="INVALID_VALUE",
    dq_description="Quantity should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Quantity Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=invalid_quantity,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

invalid_unitprice = orderline_df.filter(
    (col("unitprice").isNull()) |
    (col("unitprice") <= 0)
).count()

print("Invalid Unit Price :", invalid_unitprice)

log_dq(
    table_name="sales_orderlines",
    column_name="unitprice",
    dq_type="INVALID_VALUE",
    dq_description="Unit Price should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Unit Price Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=invalid_unitprice,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

null_lasteditedwhen = orderline_df.filter(
    col("lasteditedwhen").isNull()
).count()

print("NULL Last Edited When :", null_lasteditedwhen)

log_dq(
    table_name="sales_orderlines",
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

print("=" * 60)
print("Pre-Join DQ Checks Logged Successfully")
print("=" * 60)

spark.table(dq_logger_table) \
    .filter(col("pipeline_run_id") == pipeline_run_id) \
    .filter(col("check_category") == "PRE_JOIN") \
    .select(
        "check_name",
        "severity",
        "failed_records"
    ) \
    .show(truncate=False)

# COMMAND ----------

# ==========================================
# Task 9 : Filter Valid Order Records
# ==========================================

from pyspark.sql.functions import col, trim

orderline_valid_df = (

    orderline_df

    .filter(col("orderlineid").isNotNull())
    .filter(col("orderlineid") > 0)

    .filter(col("orderid").isNotNull())
    .filter(col("stockitemid").isNotNull())
    .filter(col("packagetypeid").isNotNull())

    .filter(col("quantity").isNotNull())
    .filter(col("quantity") > 0)

    .filter(col("unitprice").isNotNull())
    .filter(col("unitprice") > 0)

)

print("Original Records :", orderline_df.count())
print("Valid Records    :", orderline_valid_df.count())
print("Rejected Records :", orderline_df.count() - orderline_valid_df.count())

# COMMAND ----------

orderline_valid_df.select(
    "orderlineid",
    "orderid",
    "stockitemid",
    "packagetypeid",
    "quantity",
    "unitprice"
).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Task 10 : Prepare Lookup Tables
# ==========================================
# Task 10 : Prepare Lookup Tables
# ==========================================

# Orders Lookup
orders_lookup_df = (
    orders_df.select(
        "orderid",
        "backorderorderid",
        "customerid",
        "contactpersonid",
        "salespersonpersonid",
        "pickedbypersonid",
        "orderdate",
        "lasteditedwhen"
    )
    .distinct()  # Remove duplicates
)

# Customer Lookup
customer_lookup_df = (
    customer_df.select(
        "customerid",
        "customername",
        "deliverycityid"
    )
    .distinct()  # Remove duplicates
)

# City Lookup
city_lookup_df = (
    city_df.select(
        "cityid",
        "cityname"
    )
    .distinct()  # Remove duplicates
)

# People Lookup
people_lookup_df = (
    people_df.select(
        "personid",
        "fullname"
    )
    .distinct()  # Remove duplicates
)

# Stock Item Lookup
stockitem_lookup_df = (
    stockitem_df.select(
        "stockitemid",
        "stockitemname"
    )
    .distinct()  # Remove duplicates
)

# Package Type Lookup
packagetype_lookup_df = (
    packagetype_df.select(
        "packagetypeid",
        "packagetypename"
    )
    .distinct()  # Remove duplicates
)

print("=" * 60)
print("Lookup Tables Created Successfully")
print("=" * 60)

# COMMAND ----------

orders_lookup_df.show(5, truncate=False)

customer_lookup_df.show(5, truncate=False)

city_lookup_df.show(5, truncate=False)

people_lookup_df.show(5, truncate=False)

stockitem_lookup_df.show(5, truncate=False)

packagetype_lookup_df.show(5, truncate=False)

# COMMAND ----------

# ==========================================
# Task 11 : Join Lookup Tables
# ==========================================

from pyspark.sql.functions import col

order_join_df = (

    orderline_valid_df.alias("ol")

    # Order
    .join(
        orders_lookup_df.alias("o"),
        col("ol.orderid") == col("o.orderid"),
        "left"
    )

    # Customer
    .join(
        customer_lookup_df.alias("c"),
        col("o.customerid") == col("c.customerid"),
        "left"
    )

    # City
    .join(
        city_lookup_df.alias("ci"),
        col("c.deliverycityid") == col("ci.cityid"),
        "left"
    )

    # Salesperson
    .join(
        people_lookup_df.alias("sp"),
        col("o.salespersonpersonid") == col("sp.personid"),
        "left"
    )

    # Picker
    .join(
        people_lookup_df.alias("pk"),
        col("o.pickedbypersonid") == col("pk.personid"),
        "left"
    )

    # Stock Item
    .join(
        stockitem_lookup_df.alias("si"),
        col("ol.stockitemid") == col("si.stockitemid"),
        "left"
    )

    # Package Type
    .join(
        packagetype_lookup_df.alias("pt"),
        col("ol.packagetypeid") == col("pt.packagetypeid"),
        "left"
    )

    .select(

    # Order Line
    col("ol.orderlineid"),
    col("ol.orderid"),
    col("ol.stockitemid"),
    col("ol.packagetypeid"),
    col("ol.description"),
    col("ol.quantity"),
    col("ol.pickedquantity"),
    col("ol.unitprice"),
    col("ol.taxrate"),

    # Orders
    col("o.backorderorderid"),
    col("o.customerid"),
    col("o.contactpersonid"),
    col("o.salespersonpersonid"),
    col("o.pickedbypersonid"),
    col("o.orderdate"),

    # Customer
    col("c.deliverycityid"),

    # Lookup Names
    col("c.customername"),
    col("ci.cityname"),
    col("sp.fullname").alias("salesperson"),
    col("pk.fullname").alias("picked_by"),
    col("si.stockitemname").alias("stock_item"),
    col("pt.packagetypename").alias("package"),

    # Audit
    col("ol.lasteditedby"),
    col("ol.lasteditedwhen"),
    col("ol.business_month"),
    col("ol.ingestion_timestamp")

)
)

print("=" * 60)
print("Order Join Completed Successfully")
print("=" * 60)

# COMMAND ----------

orders_lookup_df.printSchema()

# COMMAND ----------

print(orders_lookup_df.columns)

# COMMAND ----------

order_join_df.printSchema()

display(order_join_df)

# COMMAND ----------

# ==========================================
# Task 12 : Post-Join Data Quality Checks
# ==========================================

post_total_records = order_join_df.count()

print("Post Join Records :", post_total_records)

# COMMAND ----------

post_missing_customer = order_join_df.filter(
    col("customername").isNull()
).count()

print("Missing Customer :", post_missing_customer)

log_dq(
    table_name="order_staging",
    column_name="customername",
    dq_type="POST_VALIDATION",
    dq_description="Customer Name should not be NULL after join",
    check_category="POST_JOIN",
    check_name="Customer Lookup Validation",
    severity="HIGH",
    total_records=post_total_records,
    failed_records=post_missing_customer,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# DBTITLE 1,Check for Duplicate Keys in Lookup Tables
# Check for duplicate keys in each lookup table

print("="*60)
print("Checking for Duplicate Keys in Lookup Tables")
print("="*60)

# Orders Lookup
orders_dups = orders_lookup_df.groupBy("orderid").count().filter(col("count") > 1).count()
print(f"Orders with duplicate orderid: {orders_dups}")

# Customer Lookup
customer_dups = customer_lookup_df.groupBy("customerid").count().filter(col("count") > 1).count()
print(f"Customers with duplicate customerid: {customer_dups}")

# City Lookup
city_dups = city_lookup_df.groupBy("cityid").count().filter(col("count") > 1).count()
print(f"Cities with duplicate cityid: {city_dups}")

# People Lookup
people_dups = people_lookup_df.groupBy("personid").count().filter(col("count") > 1).count()
print(f"People with duplicate personid: {people_dups}")

# Stock Item Lookup
stockitem_dups = stockitem_lookup_df.groupBy("stockitemid").count().filter(col("count") > 1).count()
print(f"Stock Items with duplicate stockitemid: {stockitem_dups}")

# Package Type Lookup
packagetype_dups = packagetype_lookup_df.groupBy("packagetypeid").count().filter(col("count") > 1).count()
print(f"Package Types with duplicate packagetypeid: {packagetype_dups}")

print("="*60)

# Investigate city duplicates - check if it's SCD Type 2
print("\nInvestigating City Duplicates:")
print("City DataFrame Schema:")
city_df.printSchema()

print("\nSample of duplicate cities:")
city_df.groupBy("cityid").count().filter(col("count") > 1).orderBy(col("count").desc()).show(5)

# Check a specific city with duplicates
sample_dup_city = city_df.groupBy("cityid").count().filter(col("count") > 1).first()["cityid"]
print(f"\nShowing all records for cityid={sample_dup_city}:")
city_df.filter(col("cityid") == sample_dup_city).select("cityid", "cityname", "validfrom", "validto").show(truncate=False)

# COMMAND ----------

post_missing_city = order_join_df.filter(
    col("cityname").isNull()
).count()

print("Missing City :", post_missing_city)

log_dq(
    table_name="order_staging",
    column_name="cityname",
    dq_type="POST_VALIDATION",
    dq_description="City should not be NULL after join",
    check_category="POST_JOIN",
    check_name="City Lookup Validation",
    severity="MEDIUM",
    total_records=post_total_records,
    failed_records=post_missing_city,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

post_missing_salesperson = order_join_df.filter(
    col("salesperson").isNull()
).count()

print("Missing Salesperson :", post_missing_salesperson)

log_dq(
    table_name="order_staging",
    column_name="salesperson",
    dq_type="POST_VALIDATION",
    dq_description="Salesperson should not be NULL after join",
    check_category="POST_JOIN",
    check_name="Salesperson Lookup Validation",
    severity="MEDIUM",
    total_records=post_total_records,
    failed_records=post_missing_salesperson,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

post_missing_stockitem = order_join_df.filter(
    col("stock_item").isNull()
).count()

print("Missing Stock Item :", post_missing_stockitem)

log_dq(
    table_name="order_staging",
    column_name="stock_item",
    dq_type="POST_VALIDATION",
    dq_description="Stock Item should not be NULL after join",
    check_category="POST_JOIN",
    check_name="Stock Item Lookup Validation",
    severity="HIGH",
    total_records=post_total_records,
    failed_records=post_missing_stockitem,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

post_missing_package = order_join_df.filter(
    col("package").isNull()
).count()

print("Missing Package :", post_missing_package)

log_dq(
    table_name="order_staging",
    column_name="package",
    dq_type="POST_VALIDATION",
    dq_description="Package should not be NULL after join",
    check_category="POST_JOIN",
    check_name="Package Lookup Validation",
    severity="MEDIUM",
    total_records=post_total_records,
    failed_records=post_missing_package,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

display(
    spark.table(dq_logger_table)
        .filter(col("pipeline_run_id") == pipeline_run_id)
        .filter(col("check_category") == "POST_JOIN")
        .orderBy("check_timestamp")
)

# COMMAND ----------

# ==========================================
# Task 13 : Filter Final Records
# ==========================================

from pyspark.sql.functions import col

order_final_df = (

    order_join_df

    # Mandatory lookup fields
    .filter(col("customername").isNotNull())
    .filter(col("cityname").isNotNull())
    .filter(col("salesperson").isNotNull())
    .filter(col("stock_item").isNotNull())
    .filter(col("package").isNotNull())

)

print("Records Before Filtering :", order_join_df.count())
print("Valid Silver Records     :", order_final_df.count())
print("Rejected Records         :", order_join_df.count() - order_final_df.count())

# COMMAND ----------

order_final_df.select(
    "orderlineid",
    "orderid",
    "customername",
    "cityname",
    "salesperson",
    "stock_item",
    "package"
).show(10, truncate=False)

# COMMAND ----------

# ==========================================
# Task 14 : Create Silver DataFrame
# ==========================================

from pyspark.sql.functions import *

order_silver_df = (

    order_final_df

    .select(

        col("orderid").alias("wwi_order_id"),
        col("orderlineid").alias("wwi_order_line_id"),

        # IDs
        lit(None).cast("long").alias("wwi_backorder_id"),
        col("customerid").alias("wwi_customer_id"),
        lit(None).cast("long").alias("wwi_city_id"),
        col("stockitemid").alias("wwi_stock_item_id"),
        lit(None).cast("long").alias("wwi_salesperson_id"),
        lit(None).cast("long").alias("wwi_picker_id"),
        lit(None).cast("long").alias("wwi_contact_person_id"),

        # Date Keys
        date_format(col("orderdate"), "yyyyMMdd").cast("int").alias("order_date_key"),

        date_format(col("lasteditedwhen"), "yyyyMMdd").cast("int").alias("picked_date_key"),

        # Business Columns
        col("description"),
        col("package"),
        col("quantity"),
        col("unitprice").alias("unit_price"),
        col("taxrate").alias("tax_rate"),

        # Calculated Columns
        (col("quantity") * col("unitprice")).alias("total_excluding_tax"),

        (
            col("quantity")
            * col("unitprice")
            * col("taxrate")
            / 100
        ).alias("tax_amount"),

        (
            (col("quantity") * col("unitprice"))
            +
            (
                col("quantity")
                * col("unitprice")
                * col("taxrate")
                / 100
            )
        ).alias("total_including_tax"),

        col("business_month"),

        col("lasteditedwhen").alias("last_modified_when"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp"),

        lit(pipeline_run_id).alias("pipeline_run_id")

    )

)

# COMMAND ----------

order_silver_df.printSchema()

display(order_silver_df)

# COMMAND ----------

# ==========================================
# Task 15 : Delta MERGE (SCD Type 1)
# ==========================================

from delta.tables import DeltaTable

# Get Silver Table Reference
silver_delta_table = DeltaTable.forName(
    spark,
    silver_order_table
)

# MERGE (SCD Type 1)
(
    silver_delta_table.alias("target")
    .merge(
        order_silver_df.alias("source"),
        """
        target.wwi_order_id = source.wwi_order_id
        AND target.wwi_order_line_id = source.wwi_order_line_id
        AND target.business_month = source.business_month
        """
    )
    
    # Update Existing Records
    .whenMatchedUpdate(
        set={
            "wwi_backorder_id": "source.wwi_backorder_id",
            "wwi_customer_id": "source.wwi_customer_id",
            "wwi_city_id": "source.wwi_city_id",
            "wwi_stock_item_id": "source.wwi_stock_item_id",
            "wwi_salesperson_id": "source.wwi_salesperson_id",
            "wwi_picker_id": "source.wwi_picker_id",
            "wwi_contact_person_id": "source.wwi_contact_person_id",
            "order_date_key": "source.order_date_key",
            "picked_date_key": "source.picked_date_key",
            "description": "source.description",
            "package": "source.package",
            "quantity": "source.quantity",
            "unit_price": "source.unit_price",
            "tax_rate": "source.tax_rate",
            "total_excluding_tax": "source.total_excluding_tax",
            "tax_amount": "source.tax_amount",
            "total_including_tax": "source.total_including_tax",
            "business_month": "source.business_month",
            "last_modified_when": "source.last_modified_when",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"
        }
    )

    # Insert New Records
    .whenNotMatchedInsert(
        values={
            "wwi_order_id": "source.wwi_order_id",
            "wwi_order_line_id": "source.wwi_order_line_id",
            "wwi_backorder_id": "source.wwi_backorder_id",
            "wwi_customer_id": "source.wwi_customer_id",
            "wwi_city_id": "source.wwi_city_id",
            "wwi_stock_item_id": "source.wwi_stock_item_id",
            "wwi_salesperson_id": "source.wwi_salesperson_id",
            "wwi_picker_id": "source.wwi_picker_id",
            "wwi_contact_person_id": "source.wwi_contact_person_id",
            "order_date_key": "source.order_date_key",
            "picked_date_key": "source.picked_date_key",
            "description": "source.description",
            "package": "source.package",
            "quantity": "source.quantity",
            "unit_price": "source.unit_price",
            "tax_rate": "source.tax_rate",
            "total_excluding_tax": "source.total_excluding_tax",
            "tax_amount": "source.tax_amount",
            "total_including_tax": "source.total_including_tax",
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

# ==========================================
# Task 16 : Execution Summary
# ==========================================

silver_record_count = spark.table(silver_order_table).count()

print("=" * 60)
print("Order Staging Pipeline Completed Successfully")
print("=" * 60)

print(f"Business Month        : {business_month}")
print(f"Pipeline Run ID       : {pipeline_run_id}")
print(f"Total Silver Records  : {silver_record_count}")

print("=" * 60)
print("Data successfully loaded into Silver Layer")
print("=" * 60)

# COMMAND ----------

display(
    spark.table(silver_order_table).limit(10)
)

# COMMAND ----------

spark.table(silver_order_table).select(
    "wwi_order_id",
    "wwi_order_line_id",
    "wwi_customer_id",
    "wwi_stock_item_id",
    "quantity",
    "unit_price",
    "total_excluding_tax",
    "tax_amount",
    "total_including_tax",
    "business_month"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.silver.order_staging
# MAGIC where business_month = '2016-08'

# COMMAND ----------

