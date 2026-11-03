# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.table("wwi.silver.movement_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.fact_movement").printSchema()

# COMMAND ----------

# ==========================================
# Task 1.1 : Notebook Setup
# Gold Fact - Movement
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_movement_table = "wwi.silver.movement_staging"

gold_movement_table = "wwi.gold.fact_movement"

print("="*60)
print("Gold Fact Movement Notebook Started")
print("="*60)

print("Silver Source :", silver_movement_table)
print("Gold Target   :", gold_movement_table)

# COMMAND ----------

# ==========================================
# Task 1.2 : Load Silver Data
# ==========================================

movement_df = spark.table(silver_movement_table)

print("Total Silver Records :", movement_df.count())

movement_df = (

    movement_df

    .withColumn(

        "movement_business_key",

        concat_ws(

            "-",

            col("wwi_stock_item_transaction_id"),

            col("wwi_stock_item_id")

        )

    )

)

display(movement_df)

# COMMAND ----------

# ==========================================
# Task 1.3 : Aggregate Fact Grain
# ==========================================

movement_fact_df = (

    movement_df

    .groupBy("movement_business_key")

    .agg(

        first("wwi_stock_item_transaction_id").alias("wwi_stock_item_transaction_id"),

        first("wwi_stock_item_id").alias("wwi_stock_item_id"),

        first("wwi_customer_id").alias("wwi_customer_id"),

        first("wwi_supplier_id").alias("wwi_supplier_id"),

        first("wwi_transaction_type_id").alias("wwi_transaction_type_id"),

        first("wwi_invoice_id").alias("wwi_invoice_id"),

        first("wwi_purchase_order_id").alias("wwi_purchase_order_id"),

        first("date_key").alias("date_key"),

        sum("quantity").alias("quantity"),

        max("last_modified_when").alias("last_modified_when")

    )

)

print("Fact Records :", movement_fact_df.count())

display(movement_fact_df)

# COMMAND ----------

# ==========================================
# Task 1.4 : Load Gold Dimension Tables
# ==========================================

dim_date_df = (

    spark.table("wwi.gold.dimension_date")

    .filter(col("is_current") == 1)

    .select(
        "date_key"
    )

)

dim_stock_item_df = (

    spark.table("wwi.gold.dimension_stock_item")

    .filter(col("is_current") == 1)

    .select(
        "stock_item_key",
        "wwi_stock_item_id"
    )

)

dim_customer_df = (

    spark.table("wwi.gold.dimension_customer")

    .filter(col("is_current") == 1)

    .select(
        "customer_key",
        "wwi_customer_id"
    )

)

dim_supplier_df = (

    spark.table("wwi.gold.dimension_supplier")

    .filter(col("is_current") == 1)

    .select(
        "supplier_key",
        "wwi_supplier_id"
    )

)

dim_transaction_type_df = (

    spark.table("wwi.gold.dimension_transaction_type")

    .filter(col("is_current") == 1)

    .select(
        "transaction_type_key",
        "wwi_transaction_type_id"
    )

)

print("="*60)
print("Gold Dimensions Loaded Successfully")
print("="*60)

# COMMAND ----------

# ==========================================
# Task 1.5 : Resolve Dimension Surrogate Keys
# ==========================================

movement_fact_df = (

    movement_fact_df.alias("f")

    # Stock Item
    .join(

        dim_stock_item_df.alias("si"),

        col("f.wwi_stock_item_id") == col("si.wwi_stock_item_id"),

        "left"

    )

    # Customer
    .join(

        dim_customer_df.alias("c"),

        col("f.wwi_customer_id") == col("c.wwi_customer_id"),

        "left"

    )

    # Supplier
    .join(

        dim_supplier_df.alias("s"),

        col("f.wwi_supplier_id") == col("s.wwi_supplier_id"),

        "left"

    )

    # Transaction Type
    .join(

        dim_transaction_type_df.alias("tt"),

        col("f.wwi_transaction_type_id") == col("tt.wwi_transaction_type_id"),

        "left"

    )

    .select(

        col("f.movement_business_key"),

        col("f.date_key"),

        coalesce(col("si.stock_item_key"), lit(-1)).alias("stock_item_key"),

        coalesce(col("c.customer_key"), lit(-1)).alias("customer_key"),

        coalesce(col("s.supplier_key"), lit(-1)).alias("supplier_key"),

        coalesce(col("tt.transaction_type_key"), lit(-1)).alias("transaction_type_key"),

        col("f.wwi_stock_item_transaction_id"),

        col("f.wwi_invoice_id"),

        col("f.wwi_purchase_order_id"),

        col("f.quantity"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp")

    )

)

print("="*60)
print("Fact Records After Dimension Lookup :", movement_fact_df.count())
print("="*60)

display(movement_fact_df)

# COMMAND ----------

# ==========================================
# Task 1.6 : Generate Fact Surrogate Key
# ==========================================

target_df = spark.table(gold_movement_table)

max_key = (

    target_df

    .agg(max("movement_key").alias("max_key"))

    .collect()[0]["max_key"]

)

if max_key is None:
    max_key = 0

movement_fact_df = (

    movement_fact_df

    .withColumn(

        "movement_key",

        monotonically_increasing_id() + max_key + 1

    )

)

print("Current Max Movement Key :", max_key)

display(movement_fact_df)

# COMMAND ----------

# ==========================================
# Task 1.7 : Delta MERGE into Gold Fact Movement
# ==========================================

gold_movement = DeltaTable.forName(
    spark,
    gold_movement_table
)

(

gold_movement.alias("target")

.merge(

    movement_fact_df.alias("source"),

    """
    target.wwi_stock_item_transaction_id =
    source.wwi_stock_item_transaction_id
    """

)

.whenMatchedUpdate(

    set={

        "date_key": "source.date_key",

        "stock_item_key": "source.stock_item_key",

        "customer_key": "source.customer_key",

        "supplier_key": "source.supplier_key",

        "transaction_type_key": "source.transaction_type_key",

        "wwi_invoice_id": "source.wwi_invoice_id",

        "wwi_purchase_order_id": "source.wwi_purchase_order_id",

        "quantity": "source.quantity",

        "last_updated_timestamp": "current_timestamp()"

    }

)

.whenNotMatchedInsert(

    values={

        "movement_key": "source.movement_key",

        "date_key": "source.date_key",

        "stock_item_key": "source.stock_item_key",

        "customer_key": "source.customer_key",

        "supplier_key": "source.supplier_key",

        "transaction_type_key": "source.transaction_type_key",

        "wwi_stock_item_transaction_id":
        "source.wwi_stock_item_transaction_id",

        "wwi_invoice_id":
        "source.wwi_invoice_id",

        "wwi_purchase_order_id":
        "source.wwi_purchase_order_id",

        "quantity":
        "source.quantity",

        "created_timestamp":
        "source.created_timestamp",

        "last_updated_timestamp":
        "source.last_updated_timestamp"

    }

)

.execute()

)

print("="*60)
print("Fact Movement MERGE Completed Successfully")
print("="*60)

print("Source Records :", movement_fact_df.count())

print(
    "Target Records :",
    spark.table(gold_movement_table).count()
)

# COMMAND ----------

