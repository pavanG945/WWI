# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.table("wwi.silver.sale_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.fact_sale").printSchema()

# COMMAND ----------

# Task 1.1 : Notebook Setup
# Gold Fact - Sale

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_sale_table = "wwi.silver.sale_staging"
gold_sale_table = "wwi.gold.fact_sale"

print("="*60)
print("Gold Fact Sale Notebook Started")
print("="*60)

print("Silver Source :", silver_sale_table)
print("Gold Target   :", gold_sale_table)

# COMMAND ----------

# ==========================================
# Task 1.2 : Load Silver Data
# ==========================================

sale_df = spark.table(silver_sale_table)

print("Total Silver Records :", sale_df.count())

sale_df = (

    sale_df

    .withColumn(
        "sale_business_key",
        concat_ws(
            "-",
            col("wwi_invoice_id"),
            col("wwi_stock_item_id")
        )
    )

)

display(sale_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.silver.sale_staging
# MAGIC where business_month = '2016-08'

# COMMAND ----------

# ==========================================
# Task 1.3 : Aggregate at Fact Grain
# ==========================================

sale_fact_df = (

    sale_df

    .groupBy("sale_business_key")

    .agg(

        first("wwi_invoice_id").alias("wwi_invoice_id"),

        first("wwi_stock_item_id").alias("wwi_stock_item_id"),

        first("wwi_customer_id").alias("wwi_customer_id"),

        first("wwi_bill_to_customer_id").alias("wwi_bill_to_customer_id"),

        first("wwi_city_id").alias("wwi_city_id"),

        first("wwi_salesperson_id").alias("wwi_salesperson_id"),

        first("invoice_date_key").alias("invoice_date_key"),

        first("delivery_date_key").alias("delivery_date_key"),

        first("description").alias("description"),

        first("package").alias("package"),

        sum("quantity").alias("quantity"),

        first("unit_price").alias("unit_price"),

        first("tax_rate").alias("tax_rate"),

        sum("total_excluding_tax").alias("total_excluding_tax"),

        sum("tax_amount").alias("tax_amount"),

        sum("total_including_tax").alias("total_including_tax"),

        max("last_modified_date").alias("last_modified_date")

    )

)

display(sale_fact_df)

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

print("Dimensions Loaded Successfully")

# COMMAND ----------

# ==========================================
# Task 1.5 : Resolve Dimension Keys
# ==========================================

sale_fact_df = (

    sale_fact_df.alias("f")

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
        dim_customer_df.alias("bc"),
        col("f.wwi_bill_to_customer_id")==col("bc.wwi_customer_id"),
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

    .select(

        col("f.sale_business_key"),

        coalesce(col("c.city_key"),lit(-1)).alias("city_key"),

        coalesce(col("cu.customer_key"),lit(-1)).alias("customer_key"),

        coalesce(col("bc.customer_key"),lit(-1)).alias("bill_to_customer_key"),

        coalesce(col("si.stock_item_key"),lit(-1)).alias("stock_item_key"),

        col("f.invoice_date_key").cast("int").alias("invoice_date_key"),

        col("f.delivery_date_key").cast("int").alias("delivery_date_key"),

        coalesce(col("sp.employee_key"),lit(-1)).alias("salesperson_key"),

        col("f.wwi_invoice_id"),

        col("f.description"),

        col("f.package"),

        col("f.quantity"),

        col("f.unit_price").cast("decimal(18,2)").alias("unit_price"),

        col("f.tax_rate").cast("decimal(18,2)").alias("tax_rate"),

        col("f.total_excluding_tax").cast("decimal(18,2)").alias("total_excluding_tax"),

        col("f.tax_amount").cast("decimal(18,2)").alias("tax_amount"),

        col("f.total_including_tax").cast("decimal(18,2)").alias("total_including_tax"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp")

    )

)

display(sale_fact_df)

# COMMAND ----------

# ==========================================
# Task 1.6 : Generate Sale Key
# ==========================================

max_key = (

    spark.table(gold_sale_table)

    .agg(max("sale_key").alias("max_key"))

    .collect()[0]["max_key"]

)

if max_key is None:
    max_key = 0


sale_fact_df = (

    sale_fact_df

    .withColumn(
        "sale_key",
        monotonically_increasing_id() + max_key + 1
    )

)

display(sale_fact_df)

# COMMAND ----------

# Task 1.7 : Delta MERGE

# Add sale_business_key column if it doesn't exist
try:
    spark.sql(f"ALTER TABLE {gold_sale_table} ADD COLUMN sale_business_key STRING")
    print("Added sale_business_key column to target table")
except Exception as e:
    if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
        print("sale_business_key column already exists")
    else:
        raise

gold_sale = DeltaTable.forName(spark, gold_sale_table)

(
    gold_sale.alias("target")

    .merge(

        sale_fact_df.alias("source"),

        "target.sale_business_key = source.sale_business_key"

    )

    .whenMatchedUpdate(

        set={

            "city_key":"source.city_key",

            "customer_key":"source.customer_key",

            "bill_to_customer_key":"source.bill_to_customer_key",

            "stock_item_key":"source.stock_item_key",

            "invoice_date_key":"source.invoice_date_key",

            "delivery_date_key":"source.delivery_date_key",

            "salesperson_key":"source.salesperson_key",

            "description":"source.description",

            "package":"source.package",

            "quantity":"source.quantity",

            "unit_price":"source.unit_price",

            "tax_rate":"source.tax_rate",

            "total_excluding_tax":"source.total_excluding_tax",

            "tax_amount":"source.tax_amount",

            "total_including_tax":"source.total_including_tax",

            "last_updated_timestamp":"current_timestamp()"

        }

    )

    .whenNotMatchedInsert(

        values={

            "sale_key":"source.sale_key",

            "sale_business_key":"source.sale_business_key",

            "city_key":"source.city_key",

            "customer_key":"source.customer_key",

            "bill_to_customer_key":"source.bill_to_customer_key",

            "stock_item_key":"source.stock_item_key",

            "invoice_date_key":"source.invoice_date_key",

            "delivery_date_key":"source.delivery_date_key",

            "salesperson_key":"source.salesperson_key",

            "wwi_invoice_id":"source.wwi_invoice_id",

            "description":"source.description",

            "package":"source.package",

            "quantity":"source.quantity",

            "unit_price":"source.unit_price",

            "tax_rate":"source.tax_rate",

            "total_excluding_tax":"source.total_excluding_tax",

            "tax_amount":"source.tax_amount",

            "total_including_tax":"source.total_including_tax",

            "created_timestamp":"source.created_timestamp",

            "last_updated_timestamp":"source.last_updated_timestamp"

        }

    )

    .execute()

)

print("="*60)
print("Fact Sale MERGE Completed Successfully")
print("="*60)

print("Source Records :", sale_fact_df.count())
print("Target Records :", spark.table(gold_sale_table).count())

# COMMAND ----------

print(sale_df.count())      # or sale_df.count()

# COMMAND ----------

spark.table("wwi.gold.dimension_customer") \
.groupBy("wwi_customer_id") \
.count() \
.filter("count > 1") \
.show()

# COMMAND ----------

spark.table("wwi.gold.dimension_city") \
.groupBy("wwi_city_id") \
.count() \
.filter("count > 1") \
.show()

# COMMAND ----------

spark.table("wwi.gold.dimension_employee") \
.groupBy("wwi_employee_id") \
.count() \
.filter("count > 1") \
.show()

# COMMAND ----------

spark.table("wwi.gold.dimension_stock_item") \
.groupBy("wwi_stock_item_id") \
.count() \
.filter("count > 1") \
.show()

# COMMAND ----------

