# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.table("wwi.silver.purchase_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.fact_purchase").printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_purchase_table = "wwi.silver.purchase_staging"
gold_purchase_table = "wwi.gold.fact_purchase"


# COMMAND ----------

purchase_df = spark.table(silver_purchase_table)

print("Total Silver Records :", purchase_df.count())

purchase_df = purchase_df.withColumn(
    "purchase_business_key",
    concat_ws(
        "-",
        col("wwi_purchase_order_id"),
        col("wwi_stock_item_id")
    )
)

display(purchase_df)

# COMMAND ----------

purchase_fact_df = (

    purchase_df

    .groupBy("purchase_business_key")

    .agg(

        first("wwi_purchase_order_id").alias("wwi_purchase_order_id"),

        first("wwi_stock_item_id").alias("wwi_stock_item_id"),

        first("wwi_supplier_id").alias("wwi_supplier_id"),

        first("package").alias("package"),

        first("ordered_outers").alias("ordered_outers"),

        first("ordered_quantity").alias("ordered_quantity"),

        first("received_outers").alias("received_outers"),

        first("is_order_line_finalized").alias("is_order_finalized"),

        first("date_key").alias("date_key"),

        max("last_modified_date").alias("last_modified_date")

    )

)

display(purchase_fact_df)

# COMMAND ----------

dim_supplier_df = (

    spark.table("wwi.gold.dimension_supplier")

    .filter(col("is_current") == 1)

    .select("supplier_key", "wwi_supplier_id")

)

dim_stock_item_df = (

    spark.table("wwi.gold.dimension_stock_item")

    .filter(col("is_current") == 1)

    .select("stock_item_key", "wwi_stock_item_id")

)

print("Dimensions Loaded Successfully")

# COMMAND ----------

purchase_fact_df = (

    purchase_fact_df.alias("f")

    .join(

        dim_supplier_df.alias("s"),

        col("f.wwi_supplier_id") == col("s.wwi_supplier_id"),

        "left"

    )

    .join(

        dim_stock_item_df.alias("si"),

        col("f.wwi_stock_item_id") == col("si.wwi_stock_item_id"),

        "left"

    )

    .select(

        (
            year(col("f.date_key")) * 10000 +
            month(col("f.date_key")) * 100 +
            dayofmonth(col("f.date_key"))
        ).cast("int").alias("date_key"),

        coalesce(col("s.supplier_key"), lit(-1)).alias("supplier_key"),

        coalesce(col("si.stock_item_key"), lit(-1)).alias("stock_item_key"),

        col("f.wwi_purchase_order_id"),

        col("f.ordered_outers").cast("int"),

        col("f.ordered_quantity").cast("int"),

        col("f.received_outers").cast("int"),

        col("f.package"),

        col("f.is_order_finalized"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp")

    )

)

display(purchase_fact_df)

# COMMAND ----------

max_key = (

    spark.table(gold_purchase_table)

    .agg(max("purchase_key").alias("max_key"))

    .collect()[0]["max_key"]

)

if max_key is None:
    max_key = 0

purchase_fact_df = (

    purchase_fact_df

    .withColumn(

        "purchase_key",

        monotonically_increasing_id() + max_key + 1

    )

)

display(purchase_fact_df)

# COMMAND ----------

gold_purchase = DeltaTable.forName(
    spark,
    gold_purchase_table
)

(
    gold_purchase.alias("target")

    .merge(

        purchase_fact_df.alias("source"),

        """
        target.purchase_key = source.purchase_key
        """

    )

    .whenMatchedUpdate(

        set={

            "date_key":"source.date_key",

            "supplier_key":"source.supplier_key",

            "stock_item_key":"source.stock_item_key",

            "ordered_outers":"source.ordered_outers",

            "ordered_quantity":"source.ordered_quantity",

            "received_outers":"source.received_outers",

            "package":"source.package",

            "is_order_finalized":"source.is_order_finalized",

            "last_updated_timestamp":"current_timestamp()"

        }

    )

    .whenNotMatchedInsert(

        values={

            "purchase_key":"source.purchase_key",

            "date_key":"source.date_key",

            "supplier_key":"source.supplier_key",

            "stock_item_key":"source.stock_item_key",

            "wwi_purchase_order_id":"source.wwi_purchase_order_id",

            "ordered_outers":"source.ordered_outers",

            "ordered_quantity":"source.ordered_quantity",

            "received_outers":"source.received_outers",

            "package":"source.package",

            "is_order_finalized":"source.is_order_finalized",

            "created_timestamp":"source.created_timestamp",

            "last_updated_timestamp":"source.last_updated_timestamp"

        }

    )

    .execute()

)

print("="*60)
print("Fact Purchase MERGE Completed Successfully")
print("="*60)

print("Source Records :", purchase_fact_df.count())
print("Target Records :", spark.table(gold_purchase_table).count())

# COMMAND ----------

