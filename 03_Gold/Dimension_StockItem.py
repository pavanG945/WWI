# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.table("wwi.silver.stockitem_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.dimension_stock_item").printSchema()

# COMMAND ----------

# ==========================================
# Task 1.1 : Notebook Setup
# Gold Dimension - Stock Item (SCD Type 2)
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_stock_item_table = "wwi.silver.stockitem_staging"

gold_stock_item_table = "wwi.gold.dimension_stock_item"

print("="*60)
print("Gold Stock Item Dimension Notebook Started")
print("="*60)

print("Silver Source :", silver_stock_item_table)
print("Gold Target   :", gold_stock_item_table)

# COMMAND ----------

# ==========================================
# Task 1.2 : Read Silver Data
# ==========================================

stock_item_silver_df = spark.table(silver_stock_item_table)

stock_item_silver_df.printSchema()

total_records = stock_item_silver_df.count()

print("="*60)
print("Silver Records :", total_records)
print("="*60)

if total_records == 0:
    raise Exception("Silver Stock Item table is empty.")

display(stock_item_silver_df)

# COMMAND ----------

# ==========================================
# Task 1.3 : Determine Load Type
# ==========================================

gold_stock_df = spark.table(gold_stock_item_table)

gold_count = gold_stock_df.count()

if gold_count == 0:

    load_type = "INITIAL"
    watermark = None

else:

    load_type = "INCREMENTAL"

    watermark = (

        gold_stock_df

        .agg(max("last_updated_timestamp").alias("watermark"))

        .collect()[0]["watermark"]

    )

print("="*60)
print("Load Type :", load_type)
print("Gold Records :", gold_count)
print("Watermark :", watermark)
print("="*60)

# COMMAND ----------

# ==========================================
# Task 1.4 : Initial Load
# ==========================================

if load_type == "INITIAL":

    stock_item_gold_df = (

        stock_item_silver_df

        .withColumn(
            "stock_item_key",
            monotonically_increasing_id() + 1
        )

        .withColumn("valid_from", col("last_updated_timestamp"))

        .withColumn(
            "valid_to",
            to_timestamp(lit("9999-12-31 23:59:59"))
        )

        .withColumn("is_current", lit(1))

        .withColumn(
            "updated_timestamp",
            current_timestamp()
        )

        .select(

            "stock_item_key",

            col("stock_item_id").alias("wwi_stock_item_id"),

            col("stock_item_name").alias("stock_item"),

            "color_id",
            "color_name",
            "unit_package_id",
            "unit_package_name",
            "brand",
            "lead_time_days",
            "tax_rate",
            "unit_price",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    stock_item_gold_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_stock_item_table)

    print("="*60)
    print("INITIAL LOAD COMPLETED")
    print("="*60)
    print("Inserted Records :", stock_item_gold_df.count())

else:

    print("Skipping Initial Load")

# COMMAND ----------

# ==========================================
# Task 1.5 : Incremental Load
# ==========================================

if load_type == "INCREMENTAL":

    incremental_df = (

        stock_item_silver_df

        .filter(
            col("last_updated_timestamp") > lit(watermark)
        )

    )

    incremental_count = incremental_df.count()

    print("="*60)
    print("Incremental Records :", incremental_count)
    print("="*60)

    if incremental_count == 0:
        raise Exception("No Incremental Records Found.")

else:

    print("Initial Load Completed. Stop Notebook.")

# COMMAND ----------

# ==========================================
# Task 1.6 : Detect New & Changed Records
# ==========================================

gold_current_df = (

    spark.table(gold_stock_item_table)

    .filter(col("is_current") == 1)

)

comparison_df = (

    incremental_df.alias("s")

    .join(

        gold_current_df.alias("g"),

        col("s.stock_item_id") == col("g.wwi_stock_item_id"),

        "left"

    )

)

new_records_df = (

    comparison_df

    .filter(col("g.wwi_stock_item_id").isNull())

    .select("s.*")

)

changed_records_df = (

    comparison_df

    .filter(

        col("g.wwi_stock_item_id").isNotNull()

        & (

            (col("s.stock_item_name") != col("g.stock_item")) |
            (col("s.color_id") != col("g.color_id")) |
            (col("s.color_name") != col("g.color_name")) |
            (col("s.unit_package_id") != col("g.unit_package_id")) |
            (col("s.unit_package_name") != col("g.unit_package_name")) |
            (col("s.brand") != col("g.brand")) |
            (col("s.lead_time_days") != col("g.lead_time_days")) |
            (col("s.tax_rate") != col("g.tax_rate")) |
            (col("s.unit_price") != col("g.unit_price"))

        )

    )

    .select("s.*")

)

print("New Records :", new_records_df.count())
print("Changed Records :", changed_records_df.count())

# COMMAND ----------

gold_delta = DeltaTable.forName(spark, gold_stock_item_table)

gold_delta.alias("g").merge(

    changed_records_df.alias("s"),

    "g.wwi_stock_item_id = s.stock_item_id AND g.is_current = 1"

).whenMatchedUpdate(

    set={

        "is_current": "0",

        "valid_to": "s.last_updated_timestamp",

        "updated_timestamp": "current_timestamp()"

    }

).execute()

print("Existing Stock Item records expired successfully.")

# COMMAND ----------

# ==========================================
# Task 1.7 : Insert New Versions
# ==========================================

stock_item_new_df = new_records_df.unionByName(changed_records_df)

records_to_insert = stock_item_new_df.count()

print("Records to Insert :", records_to_insert)

if records_to_insert > 0:

    max_key = (

        spark.table(gold_stock_item_table)

        .agg(max("stock_item_key").alias("max_key"))

        .collect()[0]["max_key"]

    )

    if max_key is None:
        max_key = 0

    windowSpec = Window.partitionBy(lit(1)).orderBy("stock_item_id")

    stock_item_new_df = (

        stock_item_new_df

        .withColumn(
            "stock_item_key",
            row_number().over(windowSpec) + max_key
        )

        .withColumn("valid_from", col("last_updated_timestamp"))

        .withColumn(
            "valid_to",
            to_timestamp(lit("9999-12-31 23:59:59"))
        )

        .withColumn("is_current", lit(1))

        .withColumn(
            "updated_timestamp",
            current_timestamp()
        )

        .select(

            "stock_item_key",

            col("stock_item_id").alias("wwi_stock_item_id"),

            col("stock_item_name").alias("stock_item"),

            "color_id",
            "color_name",
            "unit_package_id",
            "unit_package_name",
            "brand",
            "lead_time_days",
            "tax_rate",
            "unit_price",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    # Count before writing (Spark Connect issue with post-write counts)
    final_insert_count = stock_item_new_df.count()
    
    stock_item_new_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_stock_item_table)

    print("Inserted Records :", final_insert_count)

else:

    print("No New or Changed Records Found.")

# COMMAND ----------

# ==========================================
# Task 1.8 : Validate Gold Dimension
# ==========================================

gold_df = spark.table(gold_stock_item_table)

print("="*60)
print("Gold Stock Item Validation")
print("="*60)

print("Total Records :", gold_df.count())

print(
    "Active Records :",
    gold_df.filter(col("is_current") == 1).count()
)

print(
    "Historical Records :",
    gold_df.filter(col("is_current") == 0).count()
)

multiple_active_df = (

    gold_df

    .filter(col("is_current") == 1)

    .groupBy("wwi_stock_item_id")

    .count()

    .filter(col("count") > 1)

)

print(
    "Multiple Active Records :",
    multiple_active_df.count()
)

display(multiple_active_df)

# COMMAND ----------

display(spark.table("wwi.gold.dimension_stock_item"))

# COMMAND ----------

