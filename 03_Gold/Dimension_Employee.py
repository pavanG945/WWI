# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ==========================================
# Task 1.1 : Notebook Setup
# Gold Dimension - Employee (SCD Type 2)
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_employee_table = "wwi.silver.employee_staging"

gold_employee_table = "wwi.gold.dimension_employee"

print("="*60)
print("Gold Employee Dimension Notebook Started")
print("="*60)

print("Silver Source :", silver_employee_table)
print("Gold Target   :", gold_employee_table)

# COMMAND ----------

# ==========================================
# Task 1.2 : Read Silver Employee Data
# ==========================================

employee_silver_df = spark.table(silver_employee_table)

employee_silver_df.printSchema()

total_records = employee_silver_df.count()

print("="*60)
print("Silver Employee Records :", total_records)
print("="*60)

if total_records == 0:
    raise Exception("Employee Silver table is empty.")

display(employee_silver_df)

# COMMAND ----------

# ==========================================
# Task 1.3 : Determine Load Type
# ==========================================

gold_employee_df = spark.table(gold_employee_table)

gold_count = gold_employee_df.count()

if gold_count == 0:

    load_type = "INITIAL"
    watermark = None

else:

    load_type = "INCREMENTAL"

    watermark = (

        gold_employee_df

        .agg(
            max("last_updated_timestamp").alias("watermark")
        )

        .collect()[0]["watermark"]

    )

print("="*60)
print("Load Type :", load_type)
print("Gold Records :", gold_count)
print("Watermark :", watermark)
print("="*60)

# COMMAND ----------

spark.table("wwi.silver.employee_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.dimension_employee").printSchema()

# COMMAND ----------

# ==========================================
# Task 1.4 : Initial Load
# ==========================================

if load_type == "INITIAL":

    employee_gold_df = (

        employee_silver_df

        .withColumn(
            "employee_key",
            monotonically_increasing_id() + 1
        )

        # Missing in Silver
        .withColumn(
            "phone_number",
            lit(None).cast("string")
        )

        # SCD Type 2 Columns
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

            "employee_key",
            "wwi_employee_id",
            "employee",
            "preferred_name",
            "phone_number",
            "is_salesperson",
            "photo",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    employee_gold_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_employee_table)

    print("="*60)
    print("INITIAL LOAD COMPLETED")
    print("="*60)
    print("Inserted Records :", employee_gold_df.count())

else:

    print("Skipping Initial Load")

# COMMAND ----------

# ==========================================
# Task 1.5 : Incremental Load
# ==========================================

if load_type == "INCREMENTAL":

    incremental_df = (

        employee_silver_df

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

    print("Initial Load Already Completed")

# COMMAND ----------

# ==========================================
# Task 1.6 : Detect New & Changed Records
# ==========================================

gold_current_df = (

    spark.table(gold_employee_table)

    .filter(col("is_current") == 1)

)

comparison_df = (

    incremental_df.alias("s")

    .join(

        gold_current_df.alias("g"),

        col("s.wwi_employee_id") == col("g.wwi_employee_id"),

        "left"

    )

)

# New Employees
new_records_df = (

    comparison_df

    .filter(col("g.wwi_employee_id").isNull())

    .select("s.*")

)

# Changed Employees
changed_records_df = (

    comparison_df

    .filter(

        col("g.wwi_employee_id").isNotNull()

        & (

            (col("s.employee") != col("g.employee")) |

            (col("s.preferred_name") != col("g.preferred_name")) |

            (col("s.is_salesperson") != col("g.is_salesperson")) |

            (col("s.photo") != col("g.photo"))

        )

    )

    .select("s.*")

)

print("New Employees :", new_records_df.count())
print("Changed Employees :", changed_records_df.count())

# COMMAND ----------

gold_delta = DeltaTable.forName(spark, gold_employee_table)

gold_delta.alias("g").merge(

    changed_records_df.alias("s"),

    "g.wwi_employee_id = s.wwi_employee_id AND g.is_current = 1"

).whenMatchedUpdate(

    set={

        "is_current": "0",

        "valid_to": "s.last_updated_timestamp",

        "updated_timestamp": "current_timestamp()"

    }

).execute()

print("Existing employee records expired successfully.")

# COMMAND ----------

# ==========================================
# Task 1.7 : Insert New Employee Versions
# ==========================================

employee_new_version_df = (

    new_records_df

    .unionByName(changed_records_df)

)

records_to_insert = employee_new_version_df.count()

print("Records to Insert :", records_to_insert)

if records_to_insert > 0:

    max_employee_key = (

        spark.table(gold_employee_table)

        .agg(max("employee_key").alias("max_key"))

        .collect()[0]["max_key"]

    )

    if max_employee_key is None:
        max_employee_key = 0

    windowSpec = Window.partitionBy(lit(1)).orderBy("wwi_employee_id")

    employee_new_version_df = (

        employee_new_version_df

        .withColumn(
            "employee_key",
            row_number().over(windowSpec) + max_employee_key
        )

        .withColumn(
            "phone_number",
            lit(None).cast("string")
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

            "employee_key",
            "wwi_employee_id",
            "employee",
            "preferred_name",
            "phone_number",
            "is_salesperson",
            "photo",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    # Count before writing (Spark Connect issue with post-write counts)
    final_insert_count = employee_new_version_df.count()
    
    employee_new_version_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_employee_table)

    print("="*60)
    print("New Employee Versions Inserted Successfully")
    print("="*60)
    print("Inserted Records :", final_insert_count)

else:

    print("No New or Changed Records Found")

# COMMAND ----------

# ==========================================
# Task 1.8 : Validate Gold Employee Dimension
# ==========================================

gold_employee_df = spark.table(gold_employee_table)

total_records = gold_employee_df.count()

active_records = gold_employee_df.filter(
    col("is_current") == 1
).count()

historical_records = gold_employee_df.filter(
    col("is_current") == 0
).count()

multiple_active_df = (

    gold_employee_df

    .filter(col("is_current") == 1)

    .groupBy("wwi_employee_id")

    .count()

    .filter(col("count") > 1)

)

multiple_versions_df = (

    gold_employee_df

    .groupBy("wwi_employee_id")

    .count()

    .filter(col("count") > 1)

)

print("=" * 60)
print("Gold Employee Dimension Validation")
print("=" * 60)
print("Total Records            :", total_records)
print("Active Records           :", active_records)
print("Historical Records       :", historical_records)
print("Multiple Active Records  :", multiple_active_df.count())
print("Employees with Versions  :", multiple_versions_df.count())
print("=" * 60)

display(multiple_versions_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From wwi.gold.dimension_employee

# COMMAND ----------

