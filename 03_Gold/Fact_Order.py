# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.table("wwi.silver.order_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.fact_order").printSchema()

# COMMAND ----------

# ==========================================
# Task 1.1 : Notebook Setup
# Gold Fact - Order
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_order_table = "wwi.silver.order_staging"

gold_order_table = "wwi.gold.fact_order"

print("="*60)
print("Gold Fact Order Notebook Started")
print("="*60)

print("Silver Source :", silver_order_table)
print("Gold Target   :", gold_order_table)

# COMMAND ----------

# ==========================================
# Task 1.2 : Load Silver Data
# ==========================================

order_df = spark.table(silver_order_table)

print("Total Silver Records :", order_df.count())

order_df = (

    order_df

    .withColumn(

        "order_business_key",

        concat_ws(

            "-",

            col("wwi_order_id"),

            col("wwi_order_line_id")

        )

    )

)

display(order_df)

# COMMAND ----------

# ==========================================
# Task 1.3 : Aggregate at Fact Grain
# ==========================================

order_fact_df = (

    order_df

    .groupBy("order_business_key")

    .agg(

        first("wwi_order_id").alias("wwi_order_id"),

        first("wwi_backorder_id").alias("wwi_backorder_id"),

        first("wwi_customer_id").alias("wwi_customer_id"),

        first("wwi_city_id").alias("wwi_city_id"),

        first("wwi_stock_item_id").alias("wwi_stock_item_id"),

        first("wwi_salesperson_id").alias("wwi_salesperson_id"),

        first("wwi_picker_id").alias("wwi_picker_id"),

        first("order_date_key").alias("order_date_key"),

        first("picked_date_key").alias("picked_date_key"),

        first("description").alias("description"),

        first("package").alias("package"),

        sum("quantity").alias("quantity"),

        first("tax_rate").alias("tax_rate"),

        sum("total_excluding_tax").alias("total_excluding_tax"),

        sum("tax_amount").alias("tax_amount"),

        sum("total_including_tax").alias("total_including_tax"),

        max("last_modified_when").alias("last_modified_when")

    )

)

print("Fact Records :", order_fact_df.count())

display(order_fact_df)

# COMMAND ----------

# ==========================================
# Task 1.4 : Load Gold Dimensions
# ==========================================

dim_city_df = (

    spark.table("wwi.gold.dimension_city")

    .filter(col("is_current")==1)

    .select("city_key","wwi_city_id")

)

dim_customer_df = (

    spark.table("wwi.gold.dimension_customer")

    .filter(col("is_current")==1)

    .select("customer_key","wwi_customer_id")

)

dim_stock_item_df = (

    spark.table("wwi.gold.dimension_stock_item")

    .filter(col("is_current")==1)

    .select("stock_item_key","wwi_stock_item_id")

)

dim_employee_df = (

    spark.table("wwi.gold.dimension_employee")

    .filter(col("is_current")==1)

    .select("employee_key","wwi_employee_id")

)

dim_date_df = (

    spark.table("wwi.gold.dimension_date")

    .filter(col("is_current")==1)

    .select("date_key")

)

print("Gold Dimensions Loaded Successfully")

# COMMAND ----------

# ==========================================
# Task 1.5 : Assign Surrogate Keys
# ==========================================

order_fact_df = (

    order_fact_df.alias("f")

    .join(

        dim_city_df.alias("c"),

        col("f.wwi_city_id")==col("c.wwi_city_id"),

        "left"

    )

    .join(

        dim_customer_df.alias("cu"),

        col("f.wwi_customer_id")==col("cu.wwi_customer_id"),

        "left"

    )

    .join(

        dim_stock_item_df.alias("si"),

        col("f.wwi_stock_item_id")==col("si.wwi_stock_item_id"),

        "left"

    )

    .join(

        dim_employee_df.alias("sp"),

        col("f.wwi_salesperson_id")==col("sp.wwi_employee_id"),

        "left"

    )

    .join(

        dim_employee_df.alias("pk"),

        col("f.wwi_picker_id")==col("pk.wwi_employee_id"),

        "left"

    )

    .select(

        col("f.order_business_key"),

        coalesce(col("c.city_key"),lit(-1)).alias("city_key"),

        coalesce(col("cu.customer_key"),lit(-1)).alias("customer_key"),

        coalesce(col("si.stock_item_key"),lit(-1)).alias("stock_item_key"),

        col("f.order_date_key"),

        col("f.picked_date_key"),

        coalesce(col("sp.employee_key"),lit(-1)).alias("salesperson_key"),

        coalesce(col("pk.employee_key"),lit(-1)).alias("picker_key"),

        col("f.wwi_order_id"),

        col("f.wwi_backorder_id"),

        col("f.description"),

        col("f.package"),

        col("f.quantity"),

        col("f.tax_rate").cast("decimal(18,2)").alias("tax_rate"),

        col("f.total_excluding_tax").cast("decimal(18,2)").alias("total_excluding_tax"),

        col("f.tax_amount"),

        col("f.total_including_tax").cast("decimal(18,2)").alias("total_including_tax"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp")

    )

)

display(order_fact_df)

# COMMAND ----------

# ==========================================
# Task 1.6 : Generate Order Surrogate Key
# ==========================================

max_key = (

    spark.table(gold_order_table)

    .agg(max("order_key").alias("max_key"))

    .collect()[0]["max_key"]

)

if max_key is None:

    max_key = 0

order_fact_df = (

    order_fact_df

    .withColumn(

        "order_key",

        monotonically_increasing_id() + max_key + 1

    )

)

display(order_fact_df.limit(10))

# COMMAND ----------

# ==========================================
# Task 1.7 : Delta MERGE into Gold Fact Order
# ==========================================

gold_order = DeltaTable.forName(
    spark,
    gold_order_table
)

(
    gold_order.alias("target")

    .merge(

        order_fact_df.alias("source"),

        """
        target.order_key =
        source.order_key
        """

    )

    .whenMatchedUpdate(

        set={

            "city_key": "source.city_key",

            "customer_key": "source.customer_key",

            "stock_item_key": "source.stock_item_key",

            "order_date_key": "source.order_date_key",

            "picked_date_key": "source.picked_date_key",

            "salesperson_key": "source.salesperson_key",

            "picker_key": "source.picker_key",

            "wwi_backorder_id": "source.wwi_backorder_id",

            "description": "source.description",

            "package": "source.package",

            "quantity": "source.quantity",

            "tax_rate": "source.tax_rate",

            "total_excluding_tax": "source.total_excluding_tax",

            "tax_amount": "source.tax_amount",

            "total_including_tax": "source.total_including_tax",

            "last_updated_timestamp": "current_timestamp()"

        }

    )

    .whenNotMatchedInsert(

        values={

            "order_key": "source.order_key",

            "city_key": "source.city_key",

            "customer_key": "source.customer_key",

            "stock_item_key": "source.stock_item_key",

            "order_date_key": "source.order_date_key",

            "picked_date_key": "source.picked_date_key",

            "salesperson_key": "source.salesperson_key",

            "picker_key": "source.picker_key",

            "wwi_order_id": "source.wwi_order_id",

            "wwi_backorder_id": "source.wwi_backorder_id",

            "description": "source.description",

            "package": "source.package",

            "quantity": "source.quantity",

            "tax_rate": "source.tax_rate",

            "total_excluding_tax": "source.total_excluding_tax",

            "tax_amount": "source.tax_amount",

            "total_including_tax": "source.total_including_tax",

            "created_timestamp": "source.created_timestamp",

            "last_updated_timestamp": "source.last_updated_timestamp"

        }

    )

    .execute()

)

print("=" * 60)
print("Fact Order MERGE Completed Successfully")
print("=" * 60)

print("Source Records :", order_fact_df.count())
print("Target Records :", spark.table(gold_order_table).count())

# COMMAND ----------

