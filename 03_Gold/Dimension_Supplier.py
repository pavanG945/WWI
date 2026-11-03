# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.table("wwi.silver.supplier_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.dimension_supplier").printSchema()

# COMMAND ----------

# ==========================================
# Task 1.1 : Notebook Setup
# Gold Dimension - Supplier (SCD Type 2)
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_supplier_table = "wwi.silver.supplier_staging"

gold_supplier_table = "wwi.gold.dimension_supplier"

print("="*60)
print("Gold Supplier Dimension Notebook Started")
print("="*60)

print("Silver Source :", silver_supplier_table)
print("Gold Target   :", gold_supplier_table)

# COMMAND ----------

# ==========================================
# Task 1.2 : Read Silver Supplier Data
# ==========================================

supplier_silver_df = spark.table(silver_supplier_table)

supplier_silver_df.printSchema()

total_records = supplier_silver_df.count()

print("="*60)
print("Silver Records :", total_records)
print("="*60)

if total_records == 0:
    raise Exception("Supplier Silver table is empty.")

display(supplier_silver_df)

# COMMAND ----------

# ==========================================
# Task 1.3 : Determine Load Type
# ==========================================

gold_supplier_df = spark.table(gold_supplier_table)

gold_count = gold_supplier_df.count()

if gold_count == 0:

    load_type = "INITIAL"
    watermark = None

else:

    load_type = "INCREMENTAL"

    watermark = (

        gold_supplier_df

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

    supplier_gold_df = (

        supplier_silver_df

        .withColumn(
            "supplier_key",
            monotonically_increasing_id() + 1
        )

        .withColumn(
            "valid_from",
            col("last_updated_timestamp")
        )

        .withColumn(
            "valid_to",
            to_timestamp(lit("9999-12-31 23:59:59"))
        )

        .withColumn(
            "is_current",
            lit(1)
        )

        .withColumn(
            "updated_timestamp",
            current_timestamp()
        )

        .select(

            "supplier_key",
            "wwi_supplier_id",
            "supplier_name",
            "category",
            "primary_contact",
            "supplier_reference",
            "payment_days",
            "postal_code",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    supplier_gold_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_supplier_table)

    print("="*60)
    print("INITIAL LOAD COMPLETED")
    print("="*60)
    print("Inserted Records :", supplier_gold_df.count())

else:

    print("Skipping Initial Load")

# COMMAND ----------

# ==========================================
# Task 1.5 : Incremental Load
# ==========================================

if load_type == "INCREMENTAL":

    incremental_df = (

        supplier_silver_df

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

    spark.table(gold_supplier_table)

    .filter(col("is_current") == 1)

)

comparison_df = (

    incremental_df.alias("s")

    .join(

        gold_current_df.alias("g"),

        col("s.wwi_supplier_id") == col("g.wwi_supplier_id"),

        "left"

    )

)

new_records_df = (

    comparison_df

    .filter(col("g.wwi_supplier_id").isNull())

    .select("s.*")

)

changed_records_df = (

    comparison_df

    .filter(

        col("g.wwi_supplier_id").isNotNull()

        & (

            (col("s.supplier_name") != col("g.supplier_name")) |
            (col("s.category") != col("g.category")) |
            (col("s.primary_contact") != col("g.primary_contact")) |
            (col("s.supplier_reference") != col("g.supplier_reference")) |
            (col("s.payment_days") != col("g.payment_days")) |
            (col("s.postal_code") != col("g.postal_code"))

        )

    )

    .select("s.*")

)

print("New Records :", new_records_df.count())
print("Changed Records :", changed_records_df.count())

# COMMAND ----------

gold_delta = DeltaTable.forName(spark, gold_supplier_table)

gold_delta.alias("g").merge(

    changed_records_df.alias("s"),

    "g.wwi_supplier_id = s.wwi_supplier_id AND g.is_current = 1"

).whenMatchedUpdate(

    set={

        "is_current": "0",

        "valid_to": "s.last_updated_timestamp",

        "updated_timestamp": "current_timestamp()"

    }

).execute()

print("Existing Supplier records expired successfully.")

# COMMAND ----------

# ==========================================
# Task 1.7 : Insert New Versions
# ==========================================

supplier_new_df = new_records_df.unionByName(changed_records_df)

records_to_insert = supplier_new_df.count()

print("Records to Insert :", records_to_insert)

if records_to_insert > 0:

    max_key = (

        spark.table(gold_supplier_table)

        .agg(max("supplier_key").alias("max_key"))

        .collect()[0]["max_key"]

    )

    if max_key is None:
        max_key = 0

    windowSpec = Window.partitionBy(lit(1)).orderBy("wwi_supplier_id")

    supplier_new_df = (

        supplier_new_df

        .withColumn(
            "supplier_key",
            row_number().over(windowSpec) + max_key
        )

        .withColumn(
            "valid_from",
            col("last_updated_timestamp")
        )

        .withColumn(
            "valid_to",
            to_timestamp(lit("9999-12-31 23:59:59"))
        )

        .withColumn(
            "is_current",
            lit(1)
        )

        .withColumn(
            "updated_timestamp",
            current_timestamp()
        )

        .select(

            "supplier_key",
            "wwi_supplier_id",
            "supplier_name",
            "category",
            "primary_contact",
            "supplier_reference",
            "payment_days",
            "postal_code",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    # Count before writing (Spark Connect issue with post-write counts)
    final_insert_count = supplier_new_df.count()
    
    supplier_new_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_supplier_table)

    print("Inserted Records :", final_insert_count)

else:

    print("No New or Changed Records Found.")

# COMMAND ----------

# ==========================================
# Task 1.8 : Validate Gold Supplier Dimension
# ==========================================

gold_df = spark.table(gold_supplier_table)

print("="*60)
print("Gold Supplier Validation")
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

    .groupBy("wwi_supplier_id")

    .count()

    .filter(col("count") > 1)

)

multiple_versions_df = (

    gold_df

    .groupBy("wwi_supplier_id")

    .count()

    .filter(col("count") > 1)

)

print("Multiple Active Records :", multiple_active_df.count())
print("Suppliers with Multiple Versions :", multiple_versions_df.count())

display(multiple_versions_df)

# COMMAND ----------

