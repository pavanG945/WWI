# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ==========================================
# Task 1.1 : Notebook Setup
# Gold Dimension - Date
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_date_table = "wwi.silver.date_staging"

gold_date_table = "wwi.gold.dimension_date"

print("="*60)
print("Gold Date Dimension Notebook Started")
print("="*60)

print("Silver Source :", silver_date_table)
print("Gold Target   :", gold_date_table)

# COMMAND ----------

# ==========================================
# Task 1.2 : Read Silver Date Data
# ==========================================

date_silver_df = spark.table(silver_date_table)

date_silver_df.printSchema()

total_records = date_silver_df.count()

print("="*60)
print("Silver Records :", total_records)
print("="*60)

if total_records == 0:
    raise Exception("Silver Date table is empty.")

display(date_silver_df)

# COMMAND ----------

# ==========================================
# Task 1.3 : Determine Load Type
# ==========================================

gold_date_df = spark.table(gold_date_table)

gold_count = gold_date_df.count()

if gold_count == 0:

    load_type = "INITIAL"
    watermark = None

else:

    load_type = "INCREMENTAL"

    watermark = (

        gold_date_df

        .agg(max("last_updated_timestamp").alias("watermark"))

        .collect()[0]["watermark"]

    )

print("="*60)
print("Load Type :", load_type)
print("Gold Records :", gold_count)
print("Watermark :", watermark)
print("="*60)

# COMMAND ----------

# Silver Date Staging Schema
spark.table("wwi.silver.date_staging").printSchema()

# COMMAND ----------

# Gold Date Dimension Schema
spark.table("wwi.gold.dimension_date").printSchema()

# COMMAND ----------

# ==========================================
# Task 1.4 : Initial Load
# ==========================================

if load_type == "INITIAL":

    date_gold_df = (

        date_silver_df

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

            "id",
            "date_key",
            "date_value",
            "day_number",
            "day",
            "month_name",
            "month",
            "short_month",
            "calendar_month_number",
            "calendar_month_label",
            "calendar_year",
            "calendar_year_label",
            "fiscal_month_number",
            "fiscal_month_label",
            "fiscal_year",
            "fiscal_year_label",
            "is_weekend",
            "business_month",
            "iso_week_number",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    date_gold_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_date_table)

    print("="*60)
    print("INITIAL LOAD COMPLETED")
    print("="*60)
    print("Inserted Records :", date_gold_df.count())

else:

    print("Skipping Initial Load")

# COMMAND ----------

# ==========================================
# Task 1.5 : Incremental Load
# ==========================================

if load_type == "INCREMENTAL":

    incremental_df = (

        date_silver_df

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

    print("Initial Load Completed.")

# COMMAND ----------

# ==========================================
# Task 1.6 : Detect New & Changed Records
# ==========================================

gold_current_df = (

    spark.table(gold_date_table)

    .filter(col("is_current") == 1)

)

comparison_df = (

    incremental_df.alias("s")

    .join(

        gold_current_df.alias("g"),

        col("s.date_key") == col("g.date_key"),

        "left"

    )

)

new_records_df = (

    comparison_df

    .filter(col("g.date_key").isNull())

    .select("s.*")

)

changed_records_df = (

    comparison_df

    .filter(

        col("g.date_key").isNotNull()

        & (

            (col("s.day_number") != col("g.day_number")) |
            (col("s.day") != col("g.day")) |
            (col("s.month_name") != col("g.month_name")) |
            (col("s.month") != col("g.month")) |
            (col("s.short_month") != col("g.short_month")) |
            (col("s.calendar_month_number") != col("g.calendar_month_number")) |
            (col("s.calendar_month_label") != col("g.calendar_month_label")) |
            (col("s.calendar_year") != col("g.calendar_year")) |
            (col("s.calendar_year_label") != col("g.calendar_year_label")) |
            (col("s.fiscal_month_number") != col("g.fiscal_month_number")) |
            (col("s.fiscal_month_label") != col("g.fiscal_month_label")) |
            (col("s.fiscal_year") != col("g.fiscal_year")) |
            (col("s.fiscal_year_label") != col("g.fiscal_year_label")) |
            (col("s.is_weekend") != col("g.is_weekend")) |
            (col("s.iso_week_number") != col("g.iso_week_number"))

        )

    )

    .select("s.*")

)

print("New Records :", new_records_df.count())
print("Changed Records :", changed_records_df.count())

# COMMAND ----------

gold_delta = DeltaTable.forName(spark, gold_date_table)

gold_delta.alias("g").merge(

    changed_records_df.alias("s"),

    "g.date_key = s.date_key AND g.is_current = 1"

).whenMatchedUpdate(

    set={

        "is_current": "0",

        "valid_to": "s.last_updated_timestamp",

        "updated_timestamp": "current_timestamp()"

    }

).execute()

print("Existing Date records expired successfully.")

# COMMAND ----------

# ==========================================
# Task 1.7 : Insert New Versions
# ==========================================

date_new_df = new_records_df.unionByName(changed_records_df)

records_to_insert = date_new_df.count()

print("Records to Insert :", records_to_insert)

if records_to_insert > 0:

    date_new_df = (

        date_new_df

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

            "id",
            "date_key",
            "date_value",
            "day_number",
            "day",
            "month_name",
            "month",
            "short_month",
            "calendar_month_number",
            "calendar_month_label",
            "calendar_year",
            "calendar_year_label",
            "fiscal_month_number",
            "fiscal_month_label",
            "fiscal_year",
            "fiscal_year_label",
            "is_weekend",
            "business_month",
            "iso_week_number",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    date_new_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_date_table)

    print("Inserted Records :", date_new_df.count())

else:

    print("No New or Changed Records Found.")

# COMMAND ----------

# ==========================================
# Task 1.8 : Validate Gold Date Dimension
# ==========================================

gold_df = spark.table(gold_date_table)

print("="*60)
print("Gold Date Validation")
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

    .groupBy("date_key")

    .count()

    .filter(col("count") > 1)

)

print("Multiple Active Records :", multiple_active_df.count())

display(multiple_active_df)

# COMMAND ----------

display(spark.table(gold_date_table))

# COMMAND ----------

