# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.table("wwi.silver.stockholding_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.fact_stock_holding").printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_stockholding_table = "wwi.silver.stockholding_staging"
gold_stockholding_table = "wwi.gold.fact_stock_holding"

# COMMAND ----------

stockholding_df = spark.table(silver_stockholding_table)

print("Total Silver Records :", stockholding_df.count())

display(stockholding_df)

# COMMAND ----------

stockholding_fact_df = (

    stockholding_df

    .groupBy("wwi_stock_item_id")

    .agg(

        first("quantity_on_hand").alias("quantity_on_hand"),

        first("bin_location").alias("bin_location"),

        first("last_stocktake_quantity").alias("last_stocktake_quantity"),

        first("last_cost_price").alias("last_cost_price"),

        first("reorder_level").alias("reorder_level"),

        first("target_stock_level").alias("target_stock_level"),

        max("last_modified_date").alias("last_modified_date")

    )

)

display(stockholding_fact_df)

# COMMAND ----------

dim_stock_item_df = (

    spark.table("wwi.gold.dimension_stock_item")

    .filter(col("is_current") == 1)

    .select(

        "stock_item_key",

        "wwi_stock_item_id"

    )

)

print("Dimension Loaded Successfully")

# COMMAND ----------

stockholding_fact_df = (

    stockholding_fact_df.alias("f")

    .join(

        dim_stock_item_df.alias("d"),

        col("f.wwi_stock_item_id") == col("d.wwi_stock_item_id"),

        "left"

    )

    .select(

        coalesce(col("d.stock_item_key"), lit(-1)).alias("stock_item_key"),

        col("f.wwi_stock_item_id"),

        col("f.quantity_on_hand").cast("long"),

        col("f.bin_location"),

        col("f.last_stocktake_quantity").cast("long"),

        col("f.last_cost_price").cast("double"),

        col("f.reorder_level").cast("long"),

        col("f.target_stock_level").cast("long"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp")

    )

)

display(stockholding_fact_df)

# COMMAND ----------

max_key = (

    spark.table(gold_stockholding_table)

    .agg(max("stock_holding_key").alias("max_key"))

    .collect()[0]["max_key"]

)

if max_key is None:
    max_key = 0

stockholding_fact_df = (

    stockholding_fact_df

    .withColumn(

        "stock_holding_key",

        monotonically_increasing_id() + max_key + 1

    )

)

display(stockholding_fact_df)

# COMMAND ----------

gold_stockholding = DeltaTable.forName(

    spark,

    gold_stockholding_table

)

(

    gold_stockholding.alias("target")

    .merge(

        stockholding_fact_df.alias("source"),

        "target.stock_holding_key = source.stock_holding_key"

    )

    .whenMatchedUpdate(

        set={

            "quantity_on_hand":"source.quantity_on_hand",

            "bin_location":"source.bin_location",

            "last_stocktake_quantity":"source.last_stocktake_quantity",

            "last_cost_price":"source.last_cost_price",

            "reorder_level":"source.reorder_level",

            "target_stock_level":"source.target_stock_level",

            "last_updated_timestamp":"current_timestamp()"

        }

    )

    .whenNotMatchedInsert(

        values={

            "stock_holding_key":"source.stock_holding_key",

            "stock_item_key":"source.stock_item_key",

            "quantity_on_hand":"source.quantity_on_hand",

            "bin_location":"source.bin_location",

            "last_stocktake_quantity":"source.last_stocktake_quantity",

            "last_cost_price":"source.last_cost_price",

            "reorder_level":"source.reorder_level",

            "target_stock_level":"source.target_stock_level",

            "created_timestamp":"source.created_timestamp",

            "last_updated_timestamp":"source.last_updated_timestamp"

        }

    )

    .execute()

)

print("="*60)
print("Fact Stock Holding MERGE Completed Successfully")
print("="*60)

print("Source Records :", stockholding_fact_df.count())
print("Target Records :", spark.table(gold_stockholding_table).count())

# COMMAND ----------

