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

if business_month is None or business_month.strip() == "":
    raise Exception("Business Month is mandatory. Please enter a value (Example: 2016-06)")

print(f"Business Month : {business_month}")


# COMMAND ----------

# ==========================================
# Task 2 : Environment Setup
# ==========================================

import uuid

pipeline_run_id = str(uuid.uuid4())

# Bronze Tables
bronze_invoice_line_table = "wwi.bronze.sales_invoicelines"
bronze_invoice_table = "wwi.bronze.sales_invoices"
bronze_customer_table = "wwi.bronze.sales_customers"
bronze_city_table = "wwi.bronze.application_cities"
bronze_people_table = "wwi.bronze.application_people"
bronze_stockitem_table = "wwi.bronze.warehouse_stockitems"
bronze_packagetype_table = "wwi.bronze.warehouse_packagetypes"

# Silver Table
silver_sale_table = "wwi.silver.sale_staging"

# Logger Table
dq_logger_table = "wwi.logger.dq_logger"

print("=" * 60)
print("SALE STAGING PIPELINE STARTED")
print("=" * 60)

print(f"Business Month : {business_month}")
print(f"Pipeline Run ID: {pipeline_run_id}")

print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 3 : Verify DQ Logger
# ==========================================

spark.table(dq_logger_table).limit(5).display()

print("DQ Logger Table Found Successfully")

# COMMAND ----------

print(log_dq)

# COMMAND ----------

# ==========================================
# Task 5 : Load Bronze Tables
# ==========================================

from pyspark.sql.functions import col

invoice_line_df = (
    spark.table(bronze_invoice_line_table)
    .filter(col("business_month") == business_month)
)

invoice_df = (
    spark.table(bronze_invoice_table)
    .filter(col("business_month") == business_month)
)

customer_df = (
    spark.table(bronze_customer_table)
    .filter(col("business_month") == business_month)
)

city_df = (
    spark.table(bronze_city_table)
    .filter(col("business_month") == business_month)
)

people_df = (
    spark.table(bronze_people_table)
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

print("Invoice Lines :", invoice_line_df.count())
print("Invoices      :", invoice_df.count())
print("Customers     :", customer_df.count())
print("Cities        :", city_df.count())
print("People        :", people_df.count())
print("Stock Items   :", stockitem_df.count())
print("Package Types :", packagetype_df.count())

# COMMAND ----------

invoice_line_df.printSchema()
invoice_df.printSchema()
customer_df.printSchema()
city_df.printSchema()
people_df.printSchema()
stockitem_df.printSchema()
packagetype_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 6A : Standardize Invoice Lines
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

invoice_line_df = (

    invoice_line_df

    .withColumn("invoiceid", col("invoiceid").cast("double").cast(LongType()))
    .withColumn("invoicelineid", col("invoicelineid").cast("double").cast(LongType()))
    .withColumn("stockitemid", col("stockitemid").cast("double").cast(LongType()))
    .withColumn("packagetypeid", col("packagetypeid").cast("double").cast(LongType()))

    .withColumn("quantity", col("quantity").cast("double").cast(IntegerType()))

    .withColumn("unitprice", col("unitprice").cast("double").cast(DoubleType()))
    .withColumn("extendedprice", col("extendedprice").cast("double").cast(DoubleType()))
    .withColumn("lineprofit", col("lineprofit").cast("double").cast(DoubleType()))
    .withColumn("taxamount", col("taxamount").cast("double").cast(DoubleType()))
    .withColumn("taxrate", col("taxrate").cast("double").cast(DoubleType()))

    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast(IntegerType()))
    .withColumn("lasteditedwhen", to_timestamp("lasteditedwhen", "dd-MM-yyyy HH:mm:ss"))

    .withColumn("description", trim(initcap(col("description"))))

)

# COMMAND ----------

# ==========================================
# Task 6B : Standardize Invoices
# ==========================================

invoice_df = (

    invoice_df

    .withColumn("invoiceid", col("invoiceid").cast("double").cast(LongType()))
    .withColumn("customerid", col("customerid").cast("double").cast(LongType()))
    .withColumn("orderid", col("orderid").cast("double").cast(LongType()))
    .withColumn("contactpersonid", col("contactpersonid").cast("double").cast(LongType()))
    .withColumn("salespersonpersonid", col("salespersonpersonid").cast("double").cast(LongType()))
    .withColumn("packedbypersonid", col("packedbypersonid").cast("double").cast(LongType()))
    .withColumn("accountspersonid", col("accountspersonid").cast("double").cast(LongType()))

    .withColumn("totaldryitems", col("totaldryitems").cast("double").cast(IntegerType()))
    .withColumn("totalchilleritems", col("totalchilleritems").cast("double").cast(IntegerType()))

    .withColumn("iscreditnote", col("iscreditnote").cast("double").cast(BooleanType()))

    .withColumn("invoicedate", to_timestamp("invoicedate", "dd-MM-yyyy HH:mm:ss"))
    .withColumn("confirmeddeliverytime", to_timestamp("confirmeddeliverytime", "dd-MM-yyyy HH:mm:ss"))
    .withColumn("lasteditedwhen", to_timestamp("lasteditedwhen", "dd-MM-yyyy HH:mm:ss"))

)

# COMMAND ----------

# ==========================================
# Task 6C : Standardize Customers
# ==========================================

customer_df = (

    customer_df

    .withColumn("customerid", col("customerid").cast("double").cast(LongType()))
    .withColumn("deliverycityid", col("deliverycityid").cast("double").cast(LongType()))

    .withColumn("customername", trim(initcap(col("customername"))))

    .withColumn(
        "phonenumber",
        when(
            col("phonenumber").rlike(r'^[0-9+\-\(\) ]+$'),
            trim(col("phonenumber"))
        )
    )

    .withColumn(
        "websiteurl",
        trim(col("websiteurl"))
    )

    .withColumn(
        "deliverypostalcode",
        upper(trim(col("deliverypostalcode")))
    )

)

# COMMAND ----------

# ==========================================
# Task 6D : Standardize Cities
# ==========================================

city_df = (

    city_df

    .withColumn("cityid", col("cityid").cast("double").cast(LongType()))
    .withColumn("stateprovinceid", col("stateprovinceid").cast("double").cast(LongType()))
    .withColumn("latestrecordedpopulation", col("latestrecordedpopulation").cast("double").cast(LongType()))

    .withColumn("cityname", trim(initcap(col("cityname"))))

)

# COMMAND ----------

# ==========================================
# Task 6E : Standardize People
# ==========================================

people_df = (

    people_df

    .withColumn("personid", col("personid").cast("double").cast(LongType()))

    .withColumn("isemployee", col("isemployee").cast(BooleanType()))
    .withColumn("issalesperson", col("issalesperson").cast(BooleanType()))

    .withColumn("fullname", trim(initcap(col("fullname"))))
    .withColumn("preferredname", trim(initcap(col("preferredname"))))

    .withColumn(
        "emailaddress",
        when(
            col("emailaddress").rlike(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
            lower(trim(col("emailaddress")))
        )
    )

    .withColumn(
        "phonenumber",
        when(
            col("phonenumber").rlike(r'^[0-9+\-\(\) ]+$'),
            trim(col("phonenumber"))
        )
    )

)

# COMMAND ----------

# ==========================================
# Task 6F : Standardize Stock Items
# ==========================================

stockitem_df = (

    stockitem_df

    .withColumn("stockitemid", col("stockitemid").cast("double").cast(LongType()))
    .withColumn("supplierid", col("supplierid").cast("double").cast(LongType()))
    .withColumn("unitpackageid", col("unitpackageid").cast("double").cast(LongType()))
    .withColumn("outerpackageid", col("outerpackageid").cast("double").cast(LongType()))

    .withColumn("unitprice", col("unitprice").cast("double").cast(DoubleType()))
    .withColumn("taxrate", col("taxrate").cast("double").cast(DoubleType()))

    .withColumn(
        "stockitemname",
        trim(initcap(col("stockitemname")))
    )

)

# COMMAND ----------

# ==========================================
# Task 6G : Standardize Package Types
# ==========================================

packagetype_df = (

    packagetype_df

    .withColumn("packagetypeid", col("packagetypeid").cast("double").cast(LongType()))

    .withColumn(
        "packagetypename",
        trim(initcap(col("packagetypename")))
    )

)

# COMMAND ----------

invoice_line_df.printSchema()
invoice_df.printSchema()
customer_df.printSchema()
city_df.printSchema()
people_df.printSchema()
stockitem_df.printSchema()
packagetype_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 7 : Pre-Join DQ Checks
# ==========================================

total_records = invoice_line_df.count()

# Primary Key Checks
missing_invoice_line_id = invoice_line_df.filter(
    col("invoicelineid").isNull()
).count()

duplicate_invoice_line_id = (
    invoice_line_df.groupBy("invoicelineid")
    .count()
    .filter(col("count") > 1)
    .count()
)

# Required Field Checks
missing_invoice_id = invoice_line_df.filter(
    col("invoiceid").isNull()
).count()

missing_stockitem_id = invoice_line_df.filter(
    col("stockitemid").isNull()
).count()

missing_package_type = invoice_line_df.filter(
    col("packagetypeid").isNull()
).count()

missing_quantity = invoice_line_df.filter(
    col("quantity").isNull()
).count()

invalid_quantity = invoice_line_df.filter(
    col("quantity") <= 0
).count()

missing_unit_price = invoice_line_df.filter(
    col("unitprice").isNull()
).count()

invalid_unit_price = invoice_line_df.filter(
    col("unitprice") <= 0
).count()

# Display Summary
print("=" * 60)
print("Pre-Join DQ Summary")
print("=" * 60)

print("Total Records               :", total_records)
print("Missing Invoice Line ID     :", missing_invoice_line_id)
print("Duplicate Invoice Line ID   :", duplicate_invoice_line_id)
print("Missing Invoice ID          :", missing_invoice_id)
print("Missing Stock Item ID       :", missing_stockitem_id)
print("Missing Package Type ID     :", missing_package_type)
print("Missing Quantity            :", missing_quantity)
print("Invalid Quantity            :", invalid_quantity)
print("Missing Unit Price          :", missing_unit_price)
print("Invalid Unit Price          :", invalid_unit_price)

# COMMAND ----------

log_dq(
    table_name="sale_staging",
    column_name="invoicelineid",
    dq_type="NULL_CHECK",
    dq_description="Invoice Line ID cannot be NULL",
    check_category="PRE_JOIN",
    check_name="Invoice Line ID Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_invoice_line_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="sale_staging",
    column_name="invoiceid",
    dq_type="NULL_CHECK",
    dq_description="Invoice ID cannot be NULL",
    check_category="PRE_JOIN",
    check_name="Invoice ID Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_invoice_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="sale_staging",
    column_name="stockitemid",
    dq_type="NULL_CHECK",
    dq_description="Stock Item ID cannot be NULL",
    check_category="PRE_JOIN",
    check_name="Stock Item Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_stockitem_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="sale_staging",
    column_name="packagetypeid",
    dq_type="NULL_CHECK",
    dq_description="Package Type ID cannot be NULL",
    check_category="PRE_JOIN",
    check_name="Package Type Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_package_type,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="sale_staging",
    column_name="unitprice",
    dq_type="INVALID_VALUE",
    dq_description="Unit Price should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Unit Price Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=(missing_unit_price + invalid_unit_price),
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

# ==========================================
# Task 8 : Filter Pass Records
# ==========================================

invoice_line_valid_df = (

    invoice_line_df

    .filter(col("invoicelineid").isNotNull())
    .filter(col("invoiceid").isNotNull())
    .filter(col("stockitemid").isNotNull())
    .filter(col("packagetypeid").isNotNull())
    .filter(col("quantity").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("unitprice").isNotNull())
    .filter(col("unitprice") > 0)

)

print("=" * 60)
print("Pre-Join Filtering Summary")
print("=" * 60)

print("Original Records :", invoice_line_df.count())
print("Valid Records    :", invoice_line_valid_df.count())
print("Rejected Records :", invoice_line_df.count() - invoice_line_valid_df.count())

# COMMAND ----------

invoice_line_valid_df.printSchema()

display(invoice_line_valid_df)

# COMMAND ----------

# ==========================================
# Task 9 : Prepare Lookup Tables
# ==========================================

# Invoice Lookup
invoice_lookup_df = (

    invoice_df

    .select(
        "invoiceid",
        "customerid",
        "orderid",
        "salespersonpersonid",
        "packedbypersonid",
        "contactpersonid",
        "invoicedate",
        "lasteditedwhen"
    )

    .dropDuplicates(["invoiceid"])

)

# Customer Lookup
customer_lookup_df = (

    customer_df

    .select(
        "customerid",
        "customername",
        "deliverycityid"
    )

    .dropDuplicates(["customerid"])

)

# City Lookup
city_lookup_df = (

    city_df

    .select(
        "cityid",
        "cityname"
    )

    .dropDuplicates(["cityid"])

)

# Salesperson Lookup
salesperson_lookup_df = (

    people_df

    .filter(col("issalesperson") == True)

    .select(
        col("personid"),
        col("fullname")
    )

    .dropDuplicates(["personid"])

)

# Packed By Lookup
packedby_lookup_df = (

    people_df

    .select(
        col("personid"),
        col("fullname")
    )

    .dropDuplicates(["personid"])

)

# Stock Item Lookup
stockitem_lookup_df = (

    stockitem_df

    .select(
        "stockitemid",
        "stockitemname"
    )

    .dropDuplicates(["stockitemid"])

)

# Package Lookup
package_lookup_df = (

    packagetype_df

    .select(
        "packagetypeid",
        "packagetypename"
    )

    .dropDuplicates(["packagetypeid"])

)

print("Lookup Tables Prepared Successfully")

print("Invoice Lookup      :", invoice_lookup_df.count())
print("Customer Lookup     :", customer_lookup_df.count())
print("City Lookup         :", city_lookup_df.count())
print("Salesperson Lookup  :", salesperson_lookup_df.count())
print("PackedBy Lookup     :", packedby_lookup_df.count())
print("StockItem Lookup    :", stockitem_lookup_df.count())
print("Package Lookup      :", package_lookup_df.count())

# COMMAND ----------

# ==========================================
# Task 10 : Join Lookup Tables
# ==========================================

sale_join_df = (

    invoice_line_valid_df.alias("il")

    # Invoice
    .join(
        invoice_lookup_df.alias("i"),
        col("il.invoiceid") == col("i.invoiceid"),
        "left"
    )

    # Customer
    .join(
        customer_lookup_df.alias("c"),
        col("i.customerid") == col("c.customerid"),
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
        salesperson_lookup_df.alias("sp"),
        col("i.salespersonpersonid") == col("sp.personid"),
        "left"
    )

    # Packed By
    .join(
        packedby_lookup_df.alias("pb"),
        col("i.packedbypersonid") == col("pb.personid"),
        "left"
    )

    # Stock Item
    .join(
        stockitem_lookup_df.alias("si"),
        col("il.stockitemid") == col("si.stockitemid"),
        "left"
    )

    # Package
    .join(
        package_lookup_df.alias("pt"),
        col("il.packagetypeid") == col("pt.packagetypeid"),
        "left"
    )

    .select(

        # Invoice Line
        col("il.invoicelineid"),
        col("il.invoiceid"),
        col("il.stockitemid"),
        col("il.packagetypeid"),
        col("il.description"),
        col("il.quantity"),
        col("il.unitprice"),
        col("il.taxrate"),
        col("il.taxamount"),
        col("il.extendedprice"),
        col("il.lineprofit"),

        # Invoice
        col("i.orderid"),
        col("i.customerid"),
        col("i.contactpersonid"),
        col("i.salespersonpersonid"),
        col("i.packedbypersonid"),
        col("i.invoicedate"),
        col("i.lasteditedwhen"),

        # Customer
        col("c.customername"),
        col("c.deliverycityid"),

        # City
        col("ci.cityname"),

        # Lookup Names
        col("sp.fullname").alias("salesperson"),
        col("pb.fullname").alias("packed_by"),
        col("si.stockitemname").alias("stock_item"),
        col("pt.packagetypename").alias("package"),

        # Audit
        col("il.lasteditedby"),
        col("il.business_month"),
        col("il.ingestion_timestamp")

    )

)

print("Sale Join Completed Successfully")

# COMMAND ----------

sale_join_df.printSchema()

display(sale_join_df)

# COMMAND ----------

# ==========================================
# Task 11 : Post-Join DQ Checks
# ==========================================

total_records = sale_join_df.count()

# Missing Customer
missing_customer = sale_join_df.filter(
    col("customername").isNull()
).count()

# Missing City
missing_city = sale_join_df.filter(
    col("cityname").isNull()
).count()

# Missing Salesperson
missing_salesperson = sale_join_df.filter(
    col("salesperson").isNull()
).count()

# Missing Packed By
missing_packed_by = sale_join_df.filter(
    col("packed_by").isNull()
).count()

# Missing Stock Item
missing_stock_item = sale_join_df.filter(
    col("stock_item").isNull()
).count()

# Missing Package
missing_package = sale_join_df.filter(
    col("package").isNull()
).count()

print("=" * 60)
print("Post Join DQ Summary")
print("=" * 60)

print("Total Records       :", total_records)
print("Missing Customer    :", missing_customer)
print("Missing City        :", missing_city)
print("Missing Salesperson :", missing_salesperson)
print("Missing Packed By   :", missing_packed_by)
print("Missing Stock Item  :", missing_stock_item)
print("Missing Package     :", missing_package)

# COMMAND ----------

log_dq(
    table_name="sale_staging",
    column_name="customername",
    dq_type="MISSING_LOOKUP",
    dq_description="Customer not found in customer lookup",
    check_category="POST_JOIN",
    check_name="Customer Lookup Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_customer,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="sale_staging",
    column_name="cityname",
    dq_type="MISSING_LOOKUP",
    dq_description="City not found in city lookup",
    check_category="POST_JOIN",
    check_name="City Lookup Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_city,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="sale_staging",
    column_name="salesperson",
    dq_type="MISSING_LOOKUP",
    dq_description="Salesperson not found in people lookup",
    check_category="POST_JOIN",
    check_name="Salesperson Lookup Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_salesperson,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="sale_staging",
    column_name="packed_by",
    dq_type="MISSING_LOOKUP",
    dq_description="Packed By person not found",
    check_category="POST_JOIN",
    check_name="Packed By Lookup Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_packed_by,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="sale_staging",
    column_name="stock_item",
    dq_type="MISSING_LOOKUP",
    dq_description="Stock Item not found",
    check_category="POST_JOIN",
    check_name="Stock Item Lookup Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_stock_item,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="sale_staging",
    column_name="package",
    dq_type="MISSING_LOOKUP",
    dq_description="Package Type not found",
    check_category="POST_JOIN",
    check_name="Package Lookup Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_package,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

print("Post-Join DQ Checks Logged Successfully")

# COMMAND ----------

# ==========================================
# Task 12 : Filter Final Records
# ==========================================

sale_final_df = (

    sale_join_df

    .filter(col("customername").isNotNull())
    .filter(col("stock_item").isNotNull())
    .filter(col("salesperson").isNotNull())
    .filter(col("package").isNotNull())

)

print("=" * 60)
print("Post-Join Filtering Summary")
print("=" * 60)

print("Records Before Filtering :", sale_join_df.count())
print("Valid Silver Records     :", sale_final_df.count())
print("Rejected Records         :", sale_join_df.count() - sale_final_df.count())

# COMMAND ----------

sale_final_df.printSchema()

display(sale_final_df)

# COMMAND ----------

# ==========================================
# Task 13 : Create Silver DataFrame
# ==========================================

from pyspark.sql.functions import current_timestamp, col

sale_silver_df = (

    sale_final_df

    .select(

        # Business Keys
        col("invoiceid").alias("wwi_invoice_id"),
        col("invoicelineid").alias("wwi_invoice_line_id"),
        col("orderid").alias("wwi_order_id"),
        col("customerid").alias("wwi_bill_to_customer_id"),
        col("stockitemid").alias("wwi_stock_item_id"),
        col("salespersonpersonid").alias("wwi_salesperson_id"),
        col("packedbypersonid").alias("wwi_packed_by_person_id"),
        col("contactpersonid").alias("wwi_contact_person_id"),
        col("deliverycityid").alias("wwi_city_id"),

        # Business Attributes
        col("customername").alias("customer"),
        col("cityname").alias("city"),
        col("salesperson"),
        col("packed_by"),
        col("stock_item"),
        col("package"),

        col("description"),
        col("quantity"),
        col("unitprice").alias("unit_price"),
        col("taxrate").alias("tax_rate"),
        col("taxamount").alias("tax_amount"),
        col("extendedprice").alias("total_excluding_tax"),
        col("lineprofit").alias("line_profit"),

        col("invoicedate").alias("invoice_date"),
        col("lasteditedwhen").alias("last_modified_date"),

        col("business_month"),

        # Audit Columns
        current_timestamp().alias("created_timestamp"),
        current_timestamp().alias("last_updated_timestamp"),
        col("lasteditedby").alias("last_edited_by"),
        lit(pipeline_run_id).alias("pipeline_run_id")

    )

)

print("Silver DataFrame Created Successfully")

# COMMAND ----------

sale_silver_df.printSchema()

display(sale_silver_df)

# COMMAND ----------

spark.table("wwi.silver.sale_staging").printSchema()

# COMMAND ----------

display(spark.table("wwi.silver.sale_staging"))

# COMMAND ----------

# ==========================================
# Task 13 : Create Silver DataFrame
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

sale_silver_df = (

    sale_final_df

    .select(

        # Keys
        col("invoiceid").alias("wwi_invoice_id"),
        col("stockitemid").alias("wwi_stock_item_id"),
        col("customerid").alias("wwi_customer_id"),

        # Invoice
        col("customerid").alias("wwi_bill_to_customer_id"),
        col("deliverycityid").alias("wwi_city_id"),
        col("salespersonpersonid").alias("wwi_salesperson_id"),

        # Dates
        col("invoicedate").alias("invoice_date_key"),

        lit(None).cast("timestamp").alias("delivery_date_key"),

        # Business Columns
        col("description"),
        col("package"),
        col("quantity"),

        col("unitprice").cast(DecimalType(10,0)).alias("unit_price"),
        col("taxrate").cast(DecimalType(10,0)).alias("tax_rate"),
        col("extendedprice").cast(DecimalType(10,0)).alias("total_excluding_tax"),
        col("taxamount").cast(DecimalType(10,0)).alias("tax_amount"),
        col("lineprofit").cast(DecimalType(10,0)).alias("profit"),

        (
            col("extendedprice") + col("taxamount")
        ).cast(DecimalType(10,0)).alias("total_including_tax"),

        lit(None).cast("int").alias("total_dry_items"),
        lit(None).cast("int").alias("total_chiller_items"),

        col("lasteditedwhen").alias("last_modified_date"),

        col("business_month"),

        lit("WWI").alias("source_system"),

        current_timestamp().alias("created_timestamp"),
        current_timestamp().alias("last_updated_timestamp"),

        lit(pipeline_run_id).alias("pipeline_run_id")

    )

)

print("Sale Silver DataFrame Created Successfully")

# COMMAND ----------

# ==========================================
# Task 13 : Create Silver DataFrame
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

sale_silver_df = (

    sale_final_df

    .select(

        # Keys
        col("invoiceid").alias("wwi_invoice_id"),
        col("stockitemid").alias("wwi_stock_item_id"),
        col("customerid").alias("wwi_customer_id"),

        # Invoice
        col("customerid").alias("wwi_bill_to_customer_id"),
        col("deliverycityid").alias("wwi_city_id"),
        col("salespersonpersonid").alias("wwi_salesperson_id"),

        date_format(
            col("invoicedate"),
            "yyyyMMdd"
        ).cast("int").alias("invoice_date_key"),

        lit(None).cast("timestamp").alias("delivery_date_key"),

        # Business Columns
        col("description"),
        col("package"),
        col("quantity"),

        col("unitprice").cast(DecimalType(10,0)).alias("unit_price"),
        col("taxrate").cast(DecimalType(10,0)).alias("tax_rate"),
        col("extendedprice").cast(DecimalType(10,0)).alias("total_excluding_tax"),
        col("taxamount").cast(DecimalType(10,0)).alias("tax_amount"),
        col("lineprofit").cast(DecimalType(10,0)).alias("profit"),

        (
            col("extendedprice") + col("taxamount")
        ).cast(DecimalType(10,0)).alias("total_including_tax"),

        lit(None).cast("int").alias("total_dry_items"),
        lit(None).cast("int").alias("total_chiller_items"),

        col("lasteditedwhen").alias("last_modified_date"),

        col("business_month"),

        lit("WWI").alias("source_system"),

        current_timestamp().alias("created_timestamp"),
        current_timestamp().alias("last_updated_timestamp"),

        lit(pipeline_run_id).alias("pipeline_run_id")

    )

)

print("Sale Silver DataFrame Created Successfully")

# COMMAND ----------

sale_silver_df.printSchema()

display(sale_silver_df)

# COMMAND ----------

# ==========================================
# Task 14 : Merge into Silver Table
# ==========================================

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(
    spark,
    silver_sale_table
)

(
    silver_table.alias("target")

    .merge(
        sale_silver_df.alias("source"),
        """
        target.wwi_invoice_id = source.wwi_invoice_id
        AND target.business_month = source.business_month
        """
    )

    .whenMatchedUpdate(
        set={

            "wwi_stock_item_id": "source.wwi_stock_item_id",
            "wwi_customer_id": "source.wwi_customer_id",
            "wwi_bill_to_customer_id": "source.wwi_bill_to_customer_id",
            "wwi_city_id": "source.wwi_city_id",
            "wwi_salesperson_id": "source.wwi_salesperson_id",

            "invoice_date_key": "source.invoice_date_key",
            "delivery_date_key": "source.delivery_date_key",

            "description": "source.description",
            "package": "source.package",
            "quantity": "source.quantity",
            "unit_price": "source.unit_price",
            "tax_rate": "source.tax_rate",
            "total_excluding_tax": "source.total_excluding_tax",
            "tax_amount": "source.tax_amount",
            "profit": "source.profit",
            "total_including_tax": "source.total_including_tax",

            "total_dry_items": "source.total_dry_items",
            "total_chiller_items": "source.total_chiller_items",

            "last_modified_date": "source.last_modified_date",
            "business_month": "source.business_month",
            "source_system": "source.source_system",

            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"

        }
    )

    .whenNotMatchedInsert(
        values={

            "wwi_invoice_id": "source.wwi_invoice_id",
            "wwi_stock_item_id": "source.wwi_stock_item_id",
            "wwi_customer_id": "source.wwi_customer_id",
            "wwi_bill_to_customer_id": "source.wwi_bill_to_customer_id",
            "wwi_city_id": "source.wwi_city_id",
            "wwi_salesperson_id": "source.wwi_salesperson_id",

            "invoice_date_key": "source.invoice_date_key",
            "delivery_date_key": "source.delivery_date_key",

            "description": "source.description",
            "package": "source.package",
            "quantity": "source.quantity",
            "unit_price": "source.unit_price",
            "tax_rate": "source.tax_rate",
            "total_excluding_tax": "source.total_excluding_tax",
            "tax_amount": "source.tax_amount",
            "profit": "source.profit",
            "total_including_tax": "source.total_including_tax",

            "total_dry_items": "source.total_dry_items",
            "total_chiller_items": "source.total_chiller_items",

            "last_modified_date": "source.last_modified_date",
            "business_month": "source.business_month",
            "source_system": "source.source_system",

            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",
            "pipeline_run_id": "source.pipeline_run_id"

        }
    )

    .execute()

)

print("=" * 60)
print("Sale Staging Merge Completed Successfully")
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 15 : Verify Silver Table
# ==========================================

sale_silver_df.printSchema()

display(sale_silver_df)

print("=" * 60)
print("Silver DataFrame Verification Completed")
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

silver_record_count = (
    spark.table(silver_sale_table)
    .count()
)

print("=" * 60)
print("Sale Staging Pipeline Completed Successfully")
print("=" * 60)

print(f"Business Month        : {business_month}")
print(f"Pipeline Run ID       : {pipeline_run_id}")
print(f"Total Silver Records  : {silver_record_count}")

print("=" * 60)
print(f"Data successfully loaded into {silver_sale_table}")
print("=" * 60)

# COMMAND ----------

