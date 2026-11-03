# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_Common_Functions

# COMMAND ----------

# ==========================================
# Task 1 : Create Business Month Widget
# ==========================================

dbutils.widgets.text(
    "business_month",
    "2016-06",
    "Business Month"
)

business_month = dbutils.widgets.get("business_month").strip()

if not business_month:
    raise Exception(
        "Business Month is required. Please enter a value (e.g., 2016-06)."
    )

print("Business Month :", business_month)

# COMMAND ----------

# ==========================================
# Task 2 : Notebook Setup
# ==========================================

import uuid

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Silver Target Table
silver_date_table = "wwi.silver.date_staging"

# Centralized DQ Logger Table
dq_logger_table = "wwi.logger.dq_logger"

# Pipeline Run ID
pipeline_run_id = str(uuid.uuid4())

print("=" * 60)
print("Notebook Setup Completed")
print("=" * 60)

print("Silver Table    :", silver_date_table)
print("DQ Logger Table :", dq_logger_table)
print("Pipeline Run ID :", pipeline_run_id)

# COMMAND ----------

# ==========================================
# Task 3 : Verify DQ Logger Table
# ==========================================

try:

    spark.table(dq_logger_table)
    print(f"{dq_logger_table} is available.")

except Exception as e:

    raise Exception(
        f"DQ Logger table '{dq_logger_table}' does not exist or is not accessible.\n{e}"
    )

# COMMAND ----------

print(log_dq)

# COMMAND ----------

# ==========================================
# Task 5 : Generate Date Records
# ==========================================

from pyspark.sql.functions import *

# Convert business month (YYYY-MM) into start and end dates
start_date = f"{business_month}-01"

end_date = spark.sql(f"""
SELECT last_day(to_date('{start_date}')) AS end_date
""").collect()[0]["end_date"]

# Generate Date Range
date_df = spark.sql(f"""
SELECT
explode(
    sequence(
        to_date('{start_date}'),
        to_date('{end_date}'),
        interval 1 day
    )
) AS date_value
""")

print("=" * 60)
print("Date Dimension Generated Successfully")
print("=" * 60)

print("Business Month :", business_month)
print("Start Date     :", start_date)
print("End Date       :", end_date)
print("Total Records  :", date_df.count())

display(date_df)

# COMMAND ----------

# ==========================================
# Task 6 : Standardize Date Dimension
# ==========================================

date_df = (

    date_df

    .select(

        # Primary Keys
        (year(col("date_value")) * 10000 +
         month(col("date_value")) * 100 +
         dayofmonth(col("date_value"))).cast("integer").alias("id"),

        (year(col("date_value")) * 10000 +
         month(col("date_value")) * 100 +
         dayofmonth(col("date_value"))).cast("integer").alias("date_key"),

        col("date_value"),

        # Day Information
        dayofmonth(col("date_value")).alias("day_number"),
        dayofyear(col("date_value")).alias("day_of_year"),
        dayofmonth(col("date_value")).alias("day_of_month"),
        date_format(col("date_value"), "EEEE").alias("day"),

        # Month Information
        date_format(col("date_value"), "MMMM").alias("month_name"),
        month(col("date_value")).alias("month"),
        date_format(col("date_value"), "MMM").alias("short_month"),

        # Calendar
        month(col("date_value")).alias("calendar_month_number"),
        concat(
            lit("CY"),
            date_format(col("date_value"), "yyyy-MM")
        ).alias("calendar_month_label"),

        year(col("date_value")).alias("calendar_year"),

        concat(
            lit("CY"),
            year(col("date_value"))
        ).alias("calendar_year_label"),

        # Fiscal Calendar (July Start)

        when(
            month(col("date_value")) >= 7,
            month(col("date_value")) - 6
        ).otherwise(
            month(col("date_value")) + 6
        ).alias("fiscal_month_number"),

        concat(
            lit("FM-"),
            month(col("date_value"))
        ).alias("fiscal_month_label"),

        when(
            month(col("date_value")) >= 7,
            year(col("date_value"))
        ).otherwise(
            year(col("date_value")) - 1
        ).alias("fiscal_year"),

        concat(
            lit("FY"),
            year(col("date_value"))
        ).alias("fiscal_year_label"),

        when(
            dayofweek(col("date_value")).isin(1, 7),
            "Weekend"
        ).otherwise(
            "Weekday"
        ).alias("is_weekend"),

        weekofyear(col("date_value")).alias("iso_week_number")
    )

)

print("=" * 60)
print("Date Standardization Completed")
print("=" * 60)

date_df.printSchema()

display(date_df)

# COMMAND ----------

# ==========================================
# Task 7 : Pre-Generation DQ Checks
# ==========================================

total_records = date_df.count()

missing_date_key = date_df.filter(
    col("date_key").isNull()
).count()

missing_date_value = date_df.filter(
    col("date_value").isNull()
).count()

duplicate_date_key = (

    date_df

    .groupBy("date_key")

    .count()

    .filter(col("count") > 1)

    .count()

)

print("=" * 60)
print("Date DQ Summary")
print("=" * 60)

print("Total Records      :", total_records)
print("Missing Date Key   :", missing_date_key)
print("Missing Date Value :", missing_date_value)
print("Duplicate Date Key :", duplicate_date_key)

# COMMAND ----------

# ==========================================
# Task 8 : Log Pre-Generation DQ Results
# ==========================================

log_dq(
    table_name="date_staging",
    column_name="date_key",
    dq_type="NULL_CHECK",
    dq_description="Date Key should not be NULL",
    check_category="PRE_GENERATION",
    check_name="Date Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_date_key,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="date_staging",
    column_name="date_value",
    dq_type="NULL_CHECK",
    dq_description="Date Value should not be NULL",
    check_category="PRE_GENERATION",
    check_name="Date Value Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_date_value,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

log_dq(
    table_name="date_staging",
    column_name="date_key",
    dq_type="DUPLICATE_CHECK",
    dq_description="Date Key should be unique",
    check_category="PRE_GENERATION",
    check_name="Duplicate Date Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=duplicate_date_key,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

print("=" * 60)
print("Task 8 Completed Successfully")
print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 9 : Filter Valid Records
# ==========================================

date_valid_df = (

    date_df

    .filter(col("id").isNotNull())

    .filter(col("date_key").isNotNull())

    .filter(col("date_value").isNotNull())

)

print("=" * 60)
print("Date Filtering Summary")
print("=" * 60)

print("Original Records :", date_df.count())
print("Valid Records    :", date_valid_df.count())
print("Rejected Records :", date_df.count() - date_valid_df.count())

display(date_valid_df)

# COMMAND ----------

# ==========================================
# Task 10 : Prepare Date Dataset
# ==========================================

date_lookup_df = (

    date_valid_df

    .dropDuplicates(["date_key"])

)

print("=" * 60)
print("Date Dataset Prepared")
print("=" * 60)

print("Unique Date Records :", date_lookup_df.count())

date_lookup_df.printSchema()

display(date_lookup_df)

# COMMAND ----------

# ==========================================
# Task 11 : Join Dataset
# ==========================================

date_join_df = (

    date_lookup_df

)

print("=" * 60)
print("No Lookup Join Required")
print("=" * 60)

print("Records After Processing :", date_join_df.count())

display(date_join_df)

# COMMAND ----------

# ==========================================
# Task 12 : Post-Generation DQ Checks
# ==========================================

total_records = date_join_df.count()

missing_id = date_join_df.filter(
    col("id").isNull()
).count()

missing_date_key = date_join_df.filter(
    col("date_key").isNull()
).count()

missing_date_value = date_join_df.filter(
    col("date_value").isNull()
).count()

missing_calendar_year = date_join_df.filter(
    col("calendar_year").isNull()
).count()

missing_month_name = date_join_df.filter(
    col("month_name").isNull()
).count()

missing_day = date_join_df.filter(
    col("day").isNull()
).count()

print("=" * 60)
print("Post-Generation DQ Summary")
print("=" * 60)

print("Total Records          :", total_records)
print("Missing ID             :", missing_id)
print("Missing Date Key       :", missing_date_key)
print("Missing Date Value     :", missing_date_value)
print("Missing Calendar Year  :", missing_calendar_year)
print("Missing Month Name     :", missing_month_name)
print("Missing Day            :", missing_day)

# COMMAND ----------

# ==========================================
# Task 12 : Post-Generation DQ Checks
# ==========================================

total_records = date_join_df.count()

missing_id = date_join_df.filter(
    col("id").isNull()
).count()

missing_date_key = date_join_df.filter(
    col("date_key").isNull()
).count()

missing_date_value = date_join_df.filter(
    col("date_value").isNull()
).count()

missing_calendar_year = date_join_df.filter(
    col("calendar_year").isNull()
).count()

missing_month_name = date_join_df.filter(
    col("month_name").isNull()
).count()

missing_day = date_join_df.filter(
    col("day").isNull()
).count()

print("=" * 60)
print("Post-Generation DQ Summary")
print("=" * 60)

print("Total Records          :", total_records)
print("Missing ID             :", missing_id)
print("Missing Date Key       :", missing_date_key)
print("Missing Date Value     :", missing_date_value)
print("Missing Calendar Year  :", missing_calendar_year)
print("Missing Month Name     :", missing_month_name)
print("Missing Day            :", missing_day)

# COMMAND ----------

# ID
log_dq(
    table_name="date_staging",
    column_name="id",
    dq_type="POST_VALIDATION",
    dq_description="ID should not be NULL",
    check_category="POST_GENERATION",
    check_name="ID Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_id,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Date Key
log_dq(
    table_name="date_staging",
    column_name="date_key",
    dq_type="POST_VALIDATION",
    dq_description="Date Key should not be NULL",
    check_category="POST_GENERATION",
    check_name="Date Key Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_date_key,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Date Value
log_dq(
    table_name="date_staging",
    column_name="date_value",
    dq_type="POST_VALIDATION",
    dq_description="Date Value should not be NULL",
    check_category="POST_GENERATION",
    check_name="Date Value Validation",
    severity="HIGH",
    total_records=total_records,
    failed_records=missing_date_value,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Calendar Year
log_dq(
    table_name="date_staging",
    column_name="calendar_year",
    dq_type="POST_VALIDATION",
    dq_description="Calendar Year should not be NULL",
    check_category="POST_GENERATION",
    check_name="Calendar Year Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_calendar_year,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Month Name
log_dq(
    table_name="date_staging",
    column_name="month_name",
    dq_type="POST_VALIDATION",
    dq_description="Month Name should not be NULL",
    check_category="POST_GENERATION",
    check_name="Month Name Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_month_name,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)

# Day
log_dq(
    table_name="date_staging",
    column_name="day",
    dq_type="POST_VALIDATION",
    dq_description="Day should not be NULL",
    check_category="POST_GENERATION",
    check_name="Day Validation",
    severity="MEDIUM",
    total_records=total_records,
    failed_records=missing_day,
    pipeline_run_id=pipeline_run_id,
    dq_logger_table=dq_logger_table
)


# COMMAND ----------

# ==========================================
# Task 13 : Filter Final Records
# ==========================================

date_final_df = (

    date_join_df

    .filter(col("id").isNotNull())

    .filter(col("date_key").isNotNull())

    .filter(col("date_value").isNotNull())

    .filter(col("calendar_year").isNotNull())

    .filter(col("month_name").isNotNull())

    .filter(col("day").isNotNull())

)

print("=" * 60)
print("Final Record Summary")
print("=" * 60)

print("Records Before :", date_join_df.count())
print("Records After  :", date_final_df.count())
print("Records Removed:",
      date_join_df.count() - date_final_df.count())

display(date_final_df)

# COMMAND ----------

# ==========================================
# Task 14 : Align with Silver Schema
# ==========================================

from pyspark.sql.functions import *

date_silver_df = (

    date_final_df.select(

        col("id"),

        col("date_key"),

        col("date_value"),

        col("day_number"),

        col("day_of_year"),

        col("day_of_month"),

        col("day"),

        col("month_name"),

        col("month"),

        col("short_month"),

        col("calendar_month_number"),

        col("calendar_month_label"),

        col("calendar_year"),

        col("calendar_year_label"),

        col("fiscal_month_number"),

        col("fiscal_month_label"),

        col("fiscal_year"),

        col("fiscal_year_label"),

        col("is_weekend"),

        lit(business_month).alias("business_month"),

        col("iso_week_number"),

        current_timestamp().alias("created_timestamp"),

        current_timestamp().alias("last_updated_timestamp")

    )

)

print("=" * 60)
print("Silver Dataset Prepared")
print("=" * 60)

date_silver_df.printSchema()

display(date_silver_df)

# COMMAND ----------

# ==========================================
# Task 15 : Delta MERGE (SCD Type 1)
# ==========================================

from delta.tables import DeltaTable

silver_delta_table = DeltaTable.forName(
    spark,
    silver_date_table
)

(
    silver_delta_table.alias("target")
    .merge(
        date_silver_df.alias("source"),
        """
        target.date_key = source.date_key
        AND target.business_month = source.business_month
        """
    )

    .whenMatchedUpdate(
        set={
            "id": "source.id",
            "date_value": "source.date_value",
            "day_number": "source.day_number",
            "day_of_year": "source.day_of_year",
            "day_of_month": "source.day_of_month",
            "day": "source.day",
            "month_name": "source.month_name",
            "month": "source.month",
            "short_month": "source.short_month",
            "calendar_month_number": "source.calendar_month_number",
            "calendar_month_label": "source.calendar_month_label",
            "calendar_year": "source.calendar_year",
            "calendar_year_label": "source.calendar_year_label",
            "fiscal_month_number": "source.fiscal_month_number",
            "fiscal_month_label": "source.fiscal_month_label",
            "fiscal_year": "source.fiscal_year",
            "fiscal_year_label": "source.fiscal_year_label",
            "is_weekend": "source.is_weekend",
            "business_month": "source.business_month",
            "iso_week_number": "source.iso_week_number",
            "last_updated_timestamp": "source.last_updated_timestamp"
        }
    )

    .whenNotMatchedInsert(
        values={
            "id": "source.id",
            "date_key": "source.date_key",
            "date_value": "source.date_value",
            "day_number": "source.day_number",
            "day_of_year": "source.day_of_year",
            "day_of_month": "source.day_of_month",
            "day": "source.day",
            "month_name": "source.month_name",
            "month": "source.month",
            "short_month": "source.short_month",
            "calendar_month_number": "source.calendar_month_number",
            "calendar_month_label": "source.calendar_month_label",
            "calendar_year": "source.calendar_year",
            "calendar_year_label": "source.calendar_year_label",
            "fiscal_month_number": "source.fiscal_month_number",
            "fiscal_month_label": "source.fiscal_month_label",
            "fiscal_year": "source.fiscal_year",
            "fiscal_year_label": "source.fiscal_year_label",
            "is_weekend": "source.is_weekend",
            "business_month": "source.business_month",
            "iso_week_number": "source.iso_week_number",
            "created_timestamp": "source.created_timestamp",
            "last_updated_timestamp": "source.last_updated_timestamp"
        }
    )

    .execute()
)

print("=" * 60)
print("Task 15 : Delta MERGE Completed Successfully")
print("=" * 60)

print("Silver Date Records :",
      spark.table(silver_date_table).count())

# COMMAND ----------

# ==========================================
# Task 16 : Execution Summary
# ==========================================

from pyspark.sql import Row

total_silver_records = spark.table(
    silver_date_table
).count()

print("=" * 60)
print("      DATE STAGING PIPELINE COMPLETED")
print("=" * 60)

print(f"Pipeline Run ID      : {pipeline_run_id}")
print(f"Business Month       : {business_month}")
print(f"Silver Table         : {silver_date_table}")
print(f"Total Silver Records : {total_silver_records}")


summary_df = spark.createDataFrame([
    Row(
        pipeline_run_id=pipeline_run_id,
        business_month=business_month,
        silver_table=silver_date_table,
        total_records=total_silver_records,
        status="SUCCESS"
    )
])

display(summary_df)

# COMMAND ----------

