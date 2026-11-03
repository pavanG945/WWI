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
    "business_month",     # Widget Name
    "2016-06",            # Default Value (June 2016)
    "Business Month"      # Display Label
)

business_month = dbutils.widgets.get("business_month").strip()

if business_month == "":
    raise Exception("Business Month is Mandatory")
print(f"business_month = {business_month}")

# COMMAND ----------

# ==========================================
# Task 2 : Notebook Setup
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import uuid

# Bronze Tables
bronze_customer_transaction_table = "wwi.bronze.sales_customertransactions"
bronze_supplier_transaction_table = "wwi.bronze.purchasing_suppliertransactions"
bronze_transaction_type_table = "wwi.bronze.application_transactiontypes"
bronze_payment_method_table = "wwi.bronze.application_paymentmethods"

# Silver Table
silver_transaction_table = "wwi.silver.transaction_staging"

# DQ Logger Table
dq_logger_table = "wwi.logger.dq_logger"

# Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print(f"Business Month : {business_month}")
print(f"Pipeline Run ID: {pipeline_run_id}")

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

customer_transaction_df = (

    spark.table(bronze_customer_transaction_table)

    .filter(
        col("business_month") == business_month
    )

)

supplier_transaction_df = (

    spark.table(bronze_supplier_transaction_table)

    .filter(
        col("business_month") == business_month
    )

)

transaction_type_df = (

    spark.table(bronze_transaction_type_table)

    .filter(
        col("business_month") == business_month
    )

)

payment_method_df = (

    spark.table(bronze_payment_method_table)

    .filter(
        col("business_month") == business_month
    )

)

print("Customer Transactions :", customer_transaction_df.count())
print("Supplier Transactions :", supplier_transaction_df.count())
print("Transaction Types     :", transaction_type_df.count())
print("Payment Methods       :", payment_method_df.count())

# COMMAND ----------

customer_transaction_df.printSchema()

supplier_transaction_df.printSchema()

transaction_type_df.printSchema()

payment_method_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 6a : Standardize Customer Transactions
# ==========================================

customer_transaction_df = (

    customer_transaction_df

    # IDs
    .withColumn("customertransactionid", col("customertransactionid").cast("double").cast("long"))
    .withColumn("customerid", col("customerid").cast("double").cast("long"))
    .withColumn("invoiceid", col("invoiceid").cast("double").cast("long"))
    .withColumn("paymentmethodid", col("paymentmethodid").cast("double").cast("long"))
    .withColumn("transactiontypeid", col("transactiontypeid").cast("double").cast("long"))

    # Measures
    .withColumn("amountexcludingtax", col("amountexcludingtax").cast("double"))
    .withColumn("taxamount", col("taxamount").cast("double"))
    .withColumn("transactionamount", col("transactionamount").cast("double"))
    .withColumn("outstandingbalance", col("outstandingbalance").cast("double"))

    # Boolean
    .withColumn("isfinalized", col("isfinalized").cast("boolean"))

    # Audit
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("integer"))

    # Dates
    .withColumn("transactiondate", to_timestamp(col("transactiondate"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("finalizationdate", to_timestamp(col("finalizationdate"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("lasteditedwhen", to_timestamp(col("lasteditedwhen"), "dd-MM-yyyy HH:mm:ss"))

)


# COMMAND ----------

# ==========================================
# Task 6b : Standardize Supplier Transactions
# ==========================================

supplier_transaction_df = (

    supplier_transaction_df

    # IDs
    .withColumn("suppliertransactionid", col("suppliertransactionid").cast("double").cast("long"))
    .withColumn("supplierid", col("supplierid").cast("double").cast("long"))
    .withColumn("purchaseorderid", col("purchaseorderid").cast("double").cast("long"))
    .withColumn("paymentmethodid", col("paymentmethodid").cast("double").cast("long"))
    .withColumn("transactiontypeid", col("transactiontypeid").cast("double").cast("long"))

    .withColumn("supplierinvoicenumber", col("supplierinvoicenumber").cast("double").cast("long"))

    # Measures
    .withColumn("amountexcludingtax", col("amountexcludingtax").cast("double"))
    .withColumn("taxamount", col("taxamount").cast("double"))
    .withColumn("transactionamount", col("transactionamount").cast("double"))
    .withColumn("outstandingbalance", col("outstandingbalance").cast("double"))

    # Boolean
    .withColumn("isfinalized", col("isfinalized").cast("boolean"))

    # Audit
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("integer"))

    # Dates
    .withColumn("transactiondate", to_timestamp(col("transactiondate"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("finalizationdate",to_timestamp(col("finalizationdate"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("lasteditedwhen", to_timestamp(col("lasteditedwhen"), "dd-MM-yyyy HH:mm:ss"))

)


# COMMAND ----------

# ==========================================
# Task 6c : Standardize Transaction Types
# ==========================================

transaction_type_df = (

    transaction_type_df

    # IDs
    .withColumn("transactiontypeid", col("transactiontypeid").cast("double").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("integer"))

    # Text
    .withColumn("transactiontypename", initcap(trim(col("transactiontypename"))))

    # Dates
    .withColumn("validfrom", to_timestamp(col("validfrom"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("validto", to_timestamp(col("validto"), "dd-MM-yyyy HH:mm:ss"))

)

# COMMAND ----------

# ==========================================
# Task 6d : Standardize Payment Methods
# ==========================================

payment_method_df = (

    payment_method_df

    # IDs
    .withColumn("paymentmethodid", col("paymentmethodid").cast("double").cast("long"))
    .withColumn("lasteditedby", col("lasteditedby").cast("double").cast("integer"))

    # Text
    .withColumn("paymentmethodname", initcap(trim(col("paymentmethodname"))))

    # Dates
    .withColumn("validfrom", to_timestamp(col("validfrom"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("validto", to_timestamp(col("validto"), "dd-MM-yyyy HH:mm:ss"))

)


# COMMAND ----------

customer_transaction_df.printSchema()

supplier_transaction_df.printSchema()

transaction_type_df.printSchema()

payment_method_df.printSchema()


# COMMAND ----------

display(customer_transaction_df.limit(10))
display(supplier_transaction_df.limit(10))
display(transaction_type_df.limit(10))
display(payment_method_df.limit(10))

# COMMAND ----------

# ==========================================
# Task 7 : Pre-Join Data Quality Checks
# ==========================================

# ------------------------------------------
# Customer Transactions
# ------------------------------------------

total_customer_records = customer_transaction_df.count()

missing_customer_transaction_id = customer_transaction_df.filter(
    col("customertransactionid").isNull()
).count()

missing_customer_id = customer_transaction_df.filter(
    col("customerid").isNull()
).count()

missing_transaction_type = customer_transaction_df.filter(
    col("transactiontypeid").isNull()
).count()

missing_payment_method = customer_transaction_df.filter(
    col("paymentmethodid").isNull()
).count()

invalid_transaction_amount = customer_transaction_df.filter(
    col("transactionamount") <= 0
).count()

invalid_outstanding_balance = customer_transaction_df.filter(
    col("outstandingbalance") < 0
).count()

print("="*60)
print("Customer Transaction DQ")
print("="*60)
print("Total Records                :", total_customer_records)
print("Missing Customer Txn ID      :", missing_customer_transaction_id)
print("Missing Customer ID          :", missing_customer_id)
print("Missing Transaction Type ID  :", missing_transaction_type)
print("Missing Payment Method ID    :", missing_payment_method)
print("Invalid Transaction Amount   :", invalid_transaction_amount)
print("Invalid Outstanding Balance  :", invalid_outstanding_balance)


# ------------------------------------------
# Supplier Transactions
# ------------------------------------------

total_supplier_records = supplier_transaction_df.count()

missing_supplier_transaction_id = supplier_transaction_df.filter(
    col("suppliertransactionid").isNull()
).count()

missing_supplier_id = supplier_transaction_df.filter(
    col("supplierid").isNull()
).count()

missing_purchase_order = supplier_transaction_df.filter(
    col("purchaseorderid").isNull()
).count()

missing_supplier_transaction_type = supplier_transaction_df.filter(
    col("transactiontypeid").isNull()
).count()

missing_supplier_payment_method = supplier_transaction_df.filter(
    col("paymentmethodid").isNull()
).count()

invalid_supplier_transaction_amount = supplier_transaction_df.filter(
    col("transactionamount") <= 0
).count()

invalid_supplier_balance = supplier_transaction_df.filter(
    col("outstandingbalance") < 0
).count()

print("="*60)
print("Supplier Transaction DQ")
print("="*60)
print("Total Records                :", total_supplier_records)
print("Missing Supplier Txn ID      :", missing_supplier_transaction_id)
print("Missing Supplier ID          :", missing_supplier_id)
print("Missing Purchase Order ID    :", missing_purchase_order)
print("Missing Transaction Type ID  :", missing_supplier_transaction_type)
print("Missing Payment Method ID    :", missing_supplier_payment_method)
print("Invalid Transaction Amount   :", invalid_supplier_transaction_amount)
print("Invalid Outstanding Balance  :", invalid_supplier_balance)


# ------------------------------------------
# Lookup Table Checks
# ------------------------------------------

missing_transaction_type_lookup = transaction_type_df.filter(
    col("transactiontypeid").isNull()
).count()

duplicate_transaction_type_lookup = (
    transaction_type_df
    .groupBy("transactiontypeid")
    .count()
    .filter(col("count") > 1)
    .count()
)

missing_payment_lookup = payment_method_df.filter(
    col("paymentmethodid").isNull()
).count()

duplicate_payment_lookup = (
    payment_method_df
    .groupBy("paymentmethodid")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("="*60)
print("Lookup Table DQ")
print("="*60)
print("Missing Transaction Type IDs :", missing_transaction_type_lookup)
print("Duplicate Transaction Types  :", duplicate_transaction_type_lookup)
print("Missing Payment Method IDs   :", missing_payment_lookup)
print("Duplicate Payment Methods    :", duplicate_payment_lookup)

# COMMAND ----------

# ==========================================
# Task 8 : Log Pre-Join DQ Results
# ==========================================

# Customer Transaction ID
log_dq(
    table_name="transaction_staging",
    column_name="customertransactionid",
    dq_type="NULL_CHECK",
    dq_description="Customer Transaction ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Customer Transaction ID Validation",
    severity="HIGH",
    total_records=total_customer_records,
    failed_records=missing_customer_transaction_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Customer ID
log_dq(
    table_name="transaction_staging",
    column_name="customerid",
    dq_type="NULL_CHECK",
    dq_description="Customer ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Customer ID Validation",
    severity="HIGH",
    total_records=total_customer_records,
    failed_records=missing_customer_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Transaction Type ID
log_dq(
    table_name="transaction_staging",
    column_name="transactiontypeid",
    dq_type="NULL_CHECK",
    dq_description="Transaction Type ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Transaction Type Validation",
    severity="HIGH",
    total_records=total_customer_records,
    failed_records=missing_transaction_type,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Payment Method ID (Optional)
log_dq(
    table_name="transaction_staging",
    column_name="paymentmethodid",
    dq_type="NULL_CHECK",
    dq_description="Payment Method ID is optional",
    check_category="PRE_JOIN",
    check_name="Payment Method Validation",
    severity="LOW",
    total_records=total_customer_records,
    failed_records=missing_payment_method,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Transaction Amount
log_dq(
    table_name="transaction_staging",
    column_name="transactionamount",
    dq_type="INVALID_VALUE",
    dq_description="Transaction Amount should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Transaction Amount Validation",
    severity="HIGH",
    total_records=total_customer_records,
    failed_records=invalid_transaction_amount,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Supplier Transaction ID
log_dq(
    table_name="transaction_staging",
    column_name="suppliertransactionid",
    dq_type="NULL_CHECK",
    dq_description="Supplier Transaction ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Supplier Transaction ID Validation",
    severity="HIGH",
    total_records=total_supplier_records,
    failed_records=missing_supplier_transaction_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Supplier ID
log_dq(
    table_name="transaction_staging",
    column_name="supplierid",
    dq_type="NULL_CHECK",
    dq_description="Supplier ID should not be NULL",
    check_category="PRE_JOIN",
    check_name="Supplier ID Validation",
    severity="HIGH",
    total_records=total_supplier_records,
    failed_records=missing_supplier_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Purchase Order ID (Optional)
log_dq(
    table_name="transaction_staging",
    column_name="purchaseorderid",
    dq_type="NULL_CHECK",
    dq_description="Purchase Order ID is optional",
    check_category="PRE_JOIN",
    check_name="Purchase Order Validation",
    severity="LOW",
    total_records=total_supplier_records,
    failed_records=missing_purchase_order,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Supplier Transaction Amount
log_dq(
    table_name="transaction_staging",
    column_name="transactionamount",
    dq_type="INVALID_VALUE",
    dq_description="Supplier Transaction Amount should be greater than zero",
    check_category="PRE_JOIN",
    check_name="Supplier Transaction Amount Validation",
    severity="HIGH",
    total_records=total_supplier_records,
    failed_records=invalid_supplier_transaction_amount,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

print("=" * 60)
print("Task 8 Completed - Pre-Join DQ Results Logged")
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 9 : Prepare Lookup Tables
# ==========================================

# Customer Transaction Lookup
customer_transaction_lookup_df = (

    customer_transaction_df

    .select(
        "customertransactionid",
        "customerid",
        "invoiceid",
        "paymentmethodid",
        "transactiontypeid",
        "transactiondate",
        "amountexcludingtax",
        "taxamount",
        "transactionamount",
        "outstandingbalance",
        "isfinalized",
        "business_month",
        "lasteditedwhen"
    )

    .dropDuplicates(["customertransactionid"])

)

print("Customer Transaction Lookup Prepared")


# ------------------------------------------

# Supplier Transaction Lookup

supplier_transaction_lookup_df = (

    supplier_transaction_df

    .select(
        "suppliertransactionid",
        "supplierid",
        "purchaseorderid",
        "supplierinvoicenumber",
        "paymentmethodid",
        "transactiontypeid",
        "transactiondate",
        "isfinalized",
        "business_month",
        "lasteditedwhen"
    )

    .dropDuplicates(["suppliertransactionid"])

)

print("Supplier Transaction Lookup Prepared")


# ------------------------------------------

# Transaction Type Lookup

transaction_type_lookup_df = (

    transaction_type_df

    .select(
        "transactiontypeid",
        "transactiontypename"
    )

    .dropDuplicates(["transactiontypeid"])

)

print("Transaction Type Lookup Prepared")


# ------------------------------------------

# Payment Method Lookup

payment_method_lookup_df = (

    payment_method_df

    .select(
        "paymentmethodid",
        "paymentmethodname"
    )

    .dropDuplicates(["paymentmethodid"])

)

print("Payment Method Lookup Prepared")


print("=" * 60)
print("All Lookup Tables Prepared Successfully")
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 10 : Join Lookup Tables
# ==========================================

transaction_join_df = (

    customer_transaction_lookup_df.alias("ct")

    # Supplier Transactions
    .join(
        supplier_transaction_lookup_df.alias("st"),
        col("ct.transactiontypeid") == col("st.transactiontypeid"),
        "left"
    )

    # Transaction Types
    .join(
        transaction_type_lookup_df.alias("tt"),
        col("ct.transactiontypeid") == col("tt.transactiontypeid"),
        "left"
    )

    # Payment Methods
    .join(
        payment_method_lookup_df.alias("pm"),
        col("ct.paymentmethodid") == col("pm.paymentmethodid"),
        "left"
    )

    .select(

        # Customer Transaction
        col("ct.customertransactionid"),
        col("ct.customerid"),
        col("ct.invoiceid"),
        col("ct.transactiondate"),
        col("ct.amountexcludingtax"),
        col("ct.taxamount"),
        col("ct.transactionamount"),
        col("ct.outstandingbalance"),
        col("ct.isfinalized"),
        col("ct.paymentmethodid"),
        col("ct.transactiontypeid"),
        col("ct.business_month"),
        col("ct.lasteditedwhen"),

        # Supplier Transaction
        col("st.suppliertransactionid"),
        col("st.purchaseorderid"),
        col("st.supplierid"),
        col("st.supplierinvoicenumber"),

        # Lookup Columns
        col("tt.transactiontypename"),
        col("pm.paymentmethodname")

    )

)

print("Joined Records :", transaction_join_df.count())

transaction_join_df.printSchema()

display(transaction_join_df)

# COMMAND ----------

# ==========================================
# Task 12 : Post-Join Data Quality Checks
# ==========================================

total_records = transaction_join_df.count()

# Transaction Type Name
missing_transaction_type_name = transaction_join_df.filter(
    col("transactiontypename").isNull()
).count()

# Payment Method Name
# (Optional because paymentmethodid is optional)
missing_payment_method_name = transaction_join_df.filter(
    (col("paymentmethodid").isNotNull()) &
    (col("paymentmethodname").isNull())
).count()

print("=" * 60)
print("Post Join DQ Summary")
print("=" * 60)
print("Total Records                 :", total_records)
print("Missing Transaction Type Name :", missing_transaction_type_name)
print("Missing Payment Method Name   :", missing_payment_method_name)

# COMMAND ----------

log_dq(
    table_name="transaction_staging",
    column_name="transactiontypename",
    dq_type="POST_VALIDATION",
    dq_description="Transaction Type lookup should exist",
    check_category="POST_JOIN",
    check_name="Transaction Type Lookup Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_transaction_type_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

log_dq(
    table_name="transaction_staging",
    column_name="paymentmethodname",
    dq_type="POST_VALIDATION",
    dq_description="Payment Method lookup should exist when Payment Method ID is available",
    check_category="POST_JOIN",
    check_name="Payment Method Lookup Validation",
    severity="LOW",
    total_records=total_records,
    failed_records=missing_payment_method_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# COMMAND ----------

print("=" * 60)
print("Task 12 Completed")
print("=" * 60)
print("Missing Transaction Type Name :", missing_transaction_type_name)
print("Missing Payment Method Name   :", missing_payment_method_name)

# COMMAND ----------

# ==========================================
# Task 13 : Filter Final Records
# ==========================================

transaction_final_df = (

    transaction_join_df

    # Mandatory Lookup
    .filter(col("transactiontypename").isNotNull())

)

print("=" * 60)
print("Final Record Count")
print("=" * 60)
print("Records Before :", transaction_join_df.count())
print("Records After  :", transaction_final_df.count())
print("Records Removed:", transaction_join_df.count() - transaction_final_df.count())

display(transaction_final_df)

# COMMAND ----------

# ==========================================
# Task 14 : Align with Silver Schema
# ==========================================

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit
)
from pyspark.sql.types import DecimalType

transaction_silver_df = (

    transaction_final_df

    .select(

        # Business Keys
        col("customertransactionid").alias("wwi_customer_transaction_id"),

        lit(None).cast("long").alias("wwi_supplier_transaction_id"),

        col("invoiceid").alias("wwi_invoice_id"),

        lit(None).cast("long").alias("wwi_purchase_order_id"),

        lit(None).cast("long").alias("supplier_invoice_number"),

        # Amounts
        col("amountexcludingtax").cast(DecimalType(10,2)).alias("total_excluding_tax"),

        col("taxamount").cast(DecimalType(10,2)).alias("tax_amount"),

        col("transactionamount").cast(DecimalType(10,2)).alias("total_including_tax"),

        col("outstandingbalance").cast(DecimalType(10,2)).alias("outstanding_balance"),

        # Status
        col("isfinalized"),

        # Foreign Keys
        col("customerid").alias("wwi_customer_id"),

        lit(None).cast("long").alias("wwi_bill_to_customer_id"),

        lit(None).cast("long").alias("wwi_supplier_id"),

        col("transactiontypeid").alias("wwi_transaction_type_id"),

        col("paymentmethodid").alias("wwi_payment_method_id"),

        # Date
        col("transactiondate").alias("transaction_date"),

        # Lookup
        lit(None).cast("string").alias("supplier_name"),

        # Metadata
        col("business_month"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp"),

        lit(pipeline_run_id).alias("pipeline_run_id")

    )

)

transaction_silver_df.printSchema()

display(transaction_silver_df)

# COMMAND ----------

spark.table(silver_transaction_table).printSchema()

# COMMAND ----------

transaction_silver_df = (
    transaction_silver_df

    .withColumnRenamed("wwi_customer_transaction_id", "wwicustomertransactionid")
    .withColumnRenamed("wwi_supplier_transaction_id", "wwisuppliertransactionid")
    .withColumnRenamed("wwi_invoice_id", "wwiinvoiceid")
    .withColumnRenamed("wwi_purchase_order_id", "wwipurchaseorderid")
    .withColumnRenamed("supplier_invoice_number", "supplierinvoicenumber")

    .withColumnRenamed("total_excluding_tax", "totalexcludingtax")
    .withColumnRenamed("tax_amount", "taxamount")
    .withColumnRenamed("total_including_tax", "totalincludingtax")
    .withColumnRenamed("outstanding_balance", "outstandingbalance")

    .withColumnRenamed("wwi_customer_id", "wwicustomerid")
    .withColumnRenamed("wwi_bill_to_customer_id", "wwibilltocustomerid")
    .withColumnRenamed("wwi_supplier_id", "wwisupplierid")
    .withColumnRenamed("wwi_transaction_type_id", "wwitransactiontypeid")
    .withColumnRenamed("wwi_payment_method_id", "wwipaymentmethodid")

    .withColumnRenamed("supplier_name", "suppliername")
    .withColumnRenamed("transaction_date", "transactiondate")
)

# COMMAND ----------

transaction_silver_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 15 : Delta MERGE into Silver
# ==========================================

from delta.tables import DeltaTable

target = DeltaTable.forName(
    spark,
    silver_transaction_table
)

(
    target.alias("target")

    .merge(

        transaction_silver_df.alias("source"),

        """
        target.wwicustomertransactionid = source.wwicustomertransactionid
        AND target.business_month = source.business_month
        """

    )

    .whenMatchedUpdate(

        set={

            "transactiondate": "source.transactiondate",
            "wwiinvoiceid": "source.wwiinvoiceid",
            "wwipurchaseorderid": "source.wwipurchaseorderid",
            "supplierinvoicenumber": "source.supplierinvoicenumber",

            "totalexcludingtax": "source.totalexcludingtax",
            "taxamount": "source.taxamount",
            "totalincludingtax": "source.totalincludingtax",
            "outstandingbalance": "source.outstandingbalance",

            "isfinalized": "source.isfinalized",

            "wwicustomerid": "source.wwicustomerid",
            "wwibilltocustomerid": "source.wwibilltocustomerid",
            "wwisupplierid": "source.wwisupplierid",
            "wwitransactiontypeid": "source.wwitransactiontypeid",
            "wwipaymentmethodid": "source.wwipaymentmethodid",

            "suppliername": "source.suppliername",

            "business_month": "source.business_month",

            "last_updated_timestamp": "current_timestamp()",

            "pipeline_run_id": "source.pipeline_run_id"

        }

    )

    .whenNotMatchedInsert(

        values={

            "transactiondate": "source.transactiondate",

            "wwicustomertransactionid": "source.wwicustomertransactionid",
            "wwisuppliertransactionid": "source.wwisuppliertransactionid",
            "wwiinvoiceid": "source.wwiinvoiceid",
            "wwipurchaseorderid": "source.wwipurchaseorderid",
            "supplierinvoicenumber": "source.supplierinvoicenumber",

            "totalexcludingtax": "source.totalexcludingtax",
            "taxamount": "source.taxamount",
            "totalincludingtax": "source.totalincludingtax",
            "outstandingbalance": "source.outstandingbalance",

            "isfinalized": "source.isfinalized",

            "wwicustomerid": "source.wwicustomerid",
            "wwibilltocustomerid": "source.wwibilltocustomerid",
            "wwisupplierid": "source.wwisupplierid",
            "wwitransactiontypeid": "source.wwitransactiontypeid",
            "wwipaymentmethodid": "source.wwipaymentmethodid",

            "suppliername": "source.suppliername",

            "business_month": "source.business_month",

            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp",

            "pipeline_run_id": "source.pipeline_run_id"

        }

    )

    .execute()

)

print("=" * 60)
print("Transaction Silver Merge Completed Successfully")
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

silver_record_count = spark.table(silver_transaction_table).count()

print("=" * 60)
print("Transaction Staging Pipeline Completed Successfully")
print("=" * 60)

print(f"Business Month        : {business_month}")
print(f"Pipeline Run ID       : {pipeline_run_id}")
print(f"Total Silver Records  : {silver_record_count}")

print("=" * 60)
print("Data successfully loaded into Silver Layer")
print("=" * 60)

# COMMAND ----------

