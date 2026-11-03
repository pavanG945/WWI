# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.table("wwi.silver.transaction_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.fact_transaction").printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_transaction_table = "wwi.silver.transaction_staging"
gold_transaction_table = "wwi.gold.fact_transaction"

# COMMAND ----------

transaction_df = spark.table(silver_transaction_table)

print("Total Silver Records :", transaction_df.count())

transaction_df = transaction_df.withColumn(

    "transaction_business_key",

    concat_ws(

        "-",

        col("wwicustomertransactionid"),

        col("wwisuppliertransactionid")

    )

)

display(transaction_df)

# COMMAND ----------

transaction_fact_df = (

    transaction_df

    .groupBy("transaction_business_key")

    .agg(

        first("transactiondate").alias("transactiondate"),

        first("wwicustomertransactionid").alias("wwi_customer_transaction_id"),

        first("wwisuppliertransactionid").alias("wwi_supplier_transaction_id"),

        first("wwiinvoiceid").alias("wwi_invoice_id"),

        first("wwipurchaseorderid").alias("wwi_purchase_order_id"),

        first("supplierinvoicenumber").alias("supplier_invoice_number"),

        sum("totalexcludingtax").alias("total_excluding_tax"),

        sum("taxamount").alias("tax_amount"),

        sum("totalincludingtax").alias("total_including_tax"),

        sum("outstandingbalance").alias("outstanding_balance"),

        first("isfinalized").alias("is_finalized"),

        first("wwicustomerid").alias("wwi_customer_id"),

        first("wwibilltocustomerid").alias("wwi_bill_to_customer_id"),

        first("wwisupplierid").alias("wwi_supplier_id"),

        first("wwitransactiontypeid").alias("wwi_transaction_type_id"),

        first("wwipaymentmethodid").alias("wwi_payment_method_id"),

        max("last_updated_timestamp").alias("last_updated_timestamp")

    )

)

display(transaction_fact_df)

# COMMAND ----------

# ==========================================
# Task 1.4 : Load Gold Dimensions
# ==========================================

dim_customer_df = (
    spark.table("wwi.gold.dimension_customer")
    .filter(col("is_current") == 1)
    .select("customer_key", "wwi_customer_id")
)

dim_supplier_df = (
    spark.table("wwi.gold.dimension_supplier")
    .filter(col("is_current") == 1)
    .select("supplier_key", "wwi_supplier_id")
)

dim_transaction_type_df = (
    spark.table("wwi.gold.dimension_transaction_type")
    .filter(col("is_current") == 1)
    .select("transaction_type_key", "wwi_transaction_type_id")
)

dim_payment_method_df = (
    spark.table("wwi.gold.dimension_payment_method")
    .filter(col("is_current") == 1)
    .select("payment_method_key", "wwi_payment_method_id")
)

print("Dimensions Loaded Successfully")

# COMMAND ----------

# ==========================================
# Task 1.5 : Resolve Surrogate Keys
# ==========================================

transaction_fact_df = (

    transaction_fact_df.alias("f")

    .join(
        dim_customer_df.alias("c"),
        col("f.wwi_customer_id") == col("c.wwi_customer_id"),
        "left"
    )

    .join(
        dim_customer_df.alias("bc"),
        col("f.wwi_bill_to_customer_id") == col("bc.wwi_customer_id"),
        "left"
    )

    .join(
        dim_supplier_df.alias("s"),
        col("f.wwi_supplier_id") == col("s.wwi_supplier_id"),
        "left"
    )

    .join(
        dim_transaction_type_df.alias("tt"),
        col("f.wwi_transaction_type_id") == col("tt.wwi_transaction_type_id"),
        "left"
    )

    .join(
        dim_payment_method_df.alias("pm"),
        col("f.wwi_payment_method_id") == col("pm.wwi_payment_method_id"),
        "left"
    )

    .select(

        (
            year(col("f.transactiondate")) * 10000 +
            month(col("f.transactiondate")) * 100 +
            dayofmonth(col("f.transactiondate"))
        ).cast("int").alias("transaction_date_key"),

        coalesce(col("c.customer_key"), lit(-1)).alias("customer_key"),

        coalesce(col("bc.customer_key"), lit(-1)).alias("bill_to_customer_key"),

        coalesce(col("s.supplier_key"), lit(-1)).alias("supplier_key"),

        coalesce(col("tt.transaction_type_key"), lit(-1)).alias("transaction_type_key"),

        coalesce(col("pm.payment_method_key"), lit(-1)).alias("payment_method_key"),

        col("f.wwi_customer_transaction_id"),

        col("f.wwi_supplier_transaction_id"),

        col("f.wwi_invoice_id"),

        col("f.wwi_purchase_order_id"),

        col("f.supplier_invoice_number"),

        col("f.total_excluding_tax").cast("decimal(18,2)").alias("total_excluding_tax"),

        col("f.tax_amount").cast("decimal(18,2)").alias("tax_amount"),

        col("f.total_including_tax").cast("decimal(18,2)").alias("total_including_tax"),

        col("f.outstanding_balance").cast("decimal(18,2)").alias("outstanding_balance"),

        col("f.is_finalized"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp")

    )

)

display(transaction_fact_df)

# COMMAND ----------

# ==========================================
# Task 1.7 : Delta MERGE into Gold Fact Transaction
# ==========================================

from delta.tables import DeltaTable

gold_transaction = DeltaTable.forName(
    spark,
    gold_transaction_table
)

(
    gold_transaction.alias("target")

    .merge(

        transaction_fact_df.alias("source"),

        """
        target.wwi_customer_transaction_id = source.wwi_customer_transaction_id
        AND
        target.wwi_supplier_transaction_id = source.wwi_supplier_transaction_id
        """

    )

    .whenMatchedUpdate(
        set={

            "transaction_date_key": "source.transaction_date_key",

            "customer_key": "source.customer_key",

            "bill_to_customer_key": "source.bill_to_customer_key",

            "supplier_key": "source.supplier_key",

            "transaction_type_key": "source.transaction_type_key",

            "payment_method_key": "source.payment_method_key",

            "wwi_invoice_id": "source.wwi_invoice_id",

            "wwi_purchase_order_id": "source.wwi_purchase_order_id",

            "supplier_invoice_number": "source.supplier_invoice_number",

            "total_excluding_tax": "source.total_excluding_tax",

            "tax_amount": "source.tax_amount",

            "total_including_tax": "source.total_including_tax",

            "outstanding_balance": "source.outstanding_balance",

            "is_finalized": "source.is_finalized",

            "last_updated_timestamp": "current_timestamp()"

        }
    )

    .whenNotMatchedInsert(
        values={

            "transaction_date_key": "source.transaction_date_key",

            "customer_key": "source.customer_key",

            "bill_to_customer_key": "source.bill_to_customer_key",

            "supplier_key": "source.supplier_key",

            "transaction_type_key": "source.transaction_type_key",

            "payment_method_key": "source.payment_method_key",

            "wwi_customer_transaction_id": "source.wwi_customer_transaction_id",

            "wwi_supplier_transaction_id": "source.wwi_supplier_transaction_id",

            "wwi_invoice_id": "source.wwi_invoice_id",

            "wwi_purchase_order_id": "source.wwi_purchase_order_id",

            "supplier_invoice_number": "source.supplier_invoice_number",

            "total_excluding_tax": "source.total_excluding_tax",

            "tax_amount": "source.tax_amount",

            "total_including_tax": "source.total_including_tax",

            "outstanding_balance": "source.outstanding_balance",

            "is_finalized": "source.is_finalized",

            "created_timestamp": "source.created_timestamp",

            "last_updated_timestamp": "source.last_updated_timestamp"

        }
    )

    .execute()
)

print("=" * 60)
print("Fact Transaction MERGE Completed Successfully")
print("=" * 60)

print("Source Records :", transaction_fact_df.count())
print("Target Records :", spark.table(gold_transaction_table).count())

# COMMAND ----------

