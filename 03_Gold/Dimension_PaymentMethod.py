# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Task 1.1 : Notebook Setup
# Gold Dimension - Payment Method (SCD Type 2)

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_payment_method_table = "wwi.silver.paymentmethod_staging"

gold_payment_method_table = "wwi.gold.dimension_payment_method"

print("="*60)
print("Gold Payment Method Dimension Notebook Started")
print("="*60)

print("Silver Source :", silver_payment_method_table)
print("Gold Target   :", gold_payment_method_table)

# COMMAND ----------

spark.table("wwi.silver.paymentmethod_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.dimension_payment_method").printSchema()

# COMMAND ----------

# Task 1.2 : Read Silver Payment Method Data
payment_method_silver_df = spark.table(silver_payment_method_table)

payment_method_silver_df.printSchema()

total_records = payment_method_silver_df.count()

print("="*60)
print("Silver Records :", total_records)
print("="*60)

if total_records == 0:
    raise Exception("Silver Payment Method table is empty.")

display(payment_method_silver_df)

# COMMAND ----------

# Task 1.3 : Determine Load Type
gold_payment_df = spark.table(gold_payment_method_table)

gold_count = gold_payment_df.count()

if gold_count == 0:

    load_type = "INITIAL"
    watermark = None

else:

    load_type = "INCREMENTAL"

    watermark = (

        gold_payment_df

        .agg(max("last_updated_timestamp").alias("watermark"))

        .collect()[0]["watermark"]

    )

print("="*60)
print("Load Type :", load_type)
print("Gold Records :", gold_count)
print("Watermark :", watermark)
print("="*60)

# COMMAND ----------

# Task 1.4 : Initial Load

if load_type == "INITIAL":

    payment_method_gold_df = (

        payment_method_silver_df

        .withColumn(
            "payment_method_key",
            monotonically_increasing_id() + 1
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

            "payment_method_key",
            "wwi_payment_method_id",
            "payment_method",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    payment_method_gold_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_payment_method_table)

    print("="*60)
    print("INITIAL LOAD COMPLETED")
    print("="*60)
    print("Inserted Records :", payment_method_gold_df.count())

else:

    print("Skipping Initial Load")

# COMMAND ----------

# Task 1.5 : Incremental Load

if load_type == "INCREMENTAL":

    incremental_df = (

        payment_method_silver_df

        .filter(
            col("last_updated_timestamp") > lit(watermark)
        )

    )

    incremental_count = incremental_df.count()

    print("="*60)
    print("Incremental Records :", incremental_count)
    print("="*60)


else:

    print("Initial Load Completed. Stop Notebook.")

# COMMAND ----------

# Task 1.6 : Detect New & Changed Records
gold_current_df = (

    spark.table(gold_payment_method_table)

    .filter(col("is_current") == 1)

)

comparison_df = (

    incremental_df.alias("s")

    .join(

        gold_current_df.alias("g"),

        col("s.wwi_payment_method_id") == col("g.wwi_payment_method_id"),

        "left"

    )

)

new_records_df = (

    comparison_df

    .filter(col("g.wwi_payment_method_id").isNull())

    .select("s.*")

)

changed_records_df = (

    comparison_df

    .filter(

        col("g.wwi_payment_method_id").isNotNull()

        & (

            (col("s.payment_method") != col("g.payment_method"))

        )

    )

    .select("s.*")

)

print("New Records :", new_records_df.count())
print("Changed Records :", changed_records_df.count())

# COMMAND ----------

gold_delta = DeltaTable.forName(spark, gold_payment_method_table)

gold_delta.alias("g").merge(

    changed_records_df.alias("s"),

    "g.wwi_payment_method_id = s.wwi_payment_method_id AND g.is_current = 1"

).whenMatchedUpdate(

    set={

        "is_current": "0",

        "valid_to": "s.last_updated_timestamp",

        "updated_timestamp": "current_timestamp()"

    }

).execute()

print("Existing records expired successfully.")

# COMMAND ----------

# Task 1.7 : Insert New Versions

payment_method_new_df = new_records_df.unionByName(changed_records_df)

records_to_insert = payment_method_new_df.count()

print("Records to Insert :", records_to_insert)

if records_to_insert > 0:

    max_key = (

        spark.table(gold_payment_method_table)

        .agg(max("payment_method_key").alias("max_key"))

        .collect()[0]["max_key"]

    )

    if max_key is None:
        max_key = 0

    windowSpec = Window.partitionBy(lit(1)).orderBy("wwi_payment_method_id")

    payment_method_new_df = (

        payment_method_new_df

        .withColumn(
            "payment_method_key",
            row_number().over(windowSpec) + max_key
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

            "payment_method_key",
            "wwi_payment_method_id",
            "payment_method",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    # Count before writing (Spark Connect issue with post-write counts)
    final_insert_count = payment_method_new_df.count()
    
    payment_method_new_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_payment_method_table)

    print("Inserted Records :", final_insert_count)

else:

    print("No New or Changed Records Found.")

# COMMAND ----------

# Task 1.8 : Validate Gold Dimension

gold_df = spark.table(gold_payment_method_table)

print("="*60)
print("Gold Payment Method Validation")
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

    .groupBy("wwi_payment_method_id")

    .count()

    .filter(col("count") > 1)

)

print(
    "Multiple Active Records :",
    multiple_active_df.count()
)

display(multiple_active_df)

# COMMAND ----------

