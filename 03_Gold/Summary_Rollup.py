# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ==========================================
# D9 - Task 1.1 : Notebook Setup
# ==========================================

from pyspark.sql.functions import *
from delta.tables import DeltaTable

# Source and Target Tables
FACT_SALE_TABLE = "wwi.gold.fact_sale"
DIM_DATE_TABLE = "wwi.gold.dimension_date"
TARGET_TABLE = "wwi.gold.sales_summary_rollup"

print("=" * 60)
print("D9 - Gold Sales Summary Rollup")
print("=" * 60)

print("Fact Source  :", FACT_SALE_TABLE)
print("Date Source  :", DIM_DATE_TABLE)
print("Target Table :", TARGET_TABLE)

# COMMAND ----------

spark.table("wwi.gold.fact_sale").printSchema()

# COMMAND ----------

spark.table("wwi.gold.dimension_date").printSchema()

# COMMAND ----------

spark.table("wwi.gold.sales_summary_rollup").printSchema()

# COMMAND ----------

# ==========================================
# Task 1.2 : Load Fact and Date Dimension
# ==========================================

# Load Gold Fact Sale
fact_sale_df = spark.table(FACT_SALE_TABLE)

# Load Gold Date Dimension
dim_date_df = spark.table(DIM_DATE_TABLE)

print("=" * 60)
print("Fact Sale Records :", fact_sale_df.count())
print("Date Dimension Records :", dim_date_df.count())
print("=" * 60)

# COMMAND ----------

# Select required Fact Sale columns

fact_sale_selected_df = fact_sale_df.select(
    "wwi_invoice_id",
    "customer_key",
    "invoice_date_key",
    "quantity",
    "total_including_tax"
)

# Select required Date Dimension columns

dim_date_selected_df = dim_date_df.select(
    "date_key",
    "business_month",
    "calendar_year"
)

display(fact_sale_selected_df.limit(10))

# COMMAND ----------

dim_date_selected_df = dim_date_df.select(
    "date_key",
    "business_month",
    "calendar_month_number",
    "calendar_year"
)

display(dim_date_selected_df.limit(10))

# COMMAND ----------

# ==========================================
# Task 1.3 : Create Base Grain Dataset
# ==========================================

sales_summary_base_df = (

    fact_sale_df.alias("f")

    .join(
        dim_date_df.alias("d"),

    col("f.invoice_date_key") == col("d.date_key"),

        "left"
    )

    .select(
        col("d.business_month"),
        col("d.calendar_year"),

        # Calendar Quarter
        ceil(
            col("d.calendar_month_number") / 3
        ).cast("int").alias("calendar_quarter"),

        col("f.wwi_invoice_id"),
        col("f.customer_key"),
        col("f.quantity"),
        col("f.total_including_tax")
    )
)

# COMMAND ----------

display(
    sales_summary_base_df.limit(20)
)

# COMMAND ----------

print(
    "Total Records :",
    sales_summary_base_df.count()
)

print(
    "Records with NULL business_month :",
    sales_summary_base_df
        .filter(col("business_month").isNull())
        .count()
)

# COMMAND ----------

sales_summary_base_df.groupBy(
    "business_month",
    "calendar_year",
    "calendar_quarter"
).count().show()

# COMMAND ----------

# Create reusable temporary view

sales_summary_base_df.createOrReplaceTempView(
    "vw_sales_summary_base"
)

print("Temporary View Created Successfully:")
print("vw_sales_summary_base")

# COMMAND ----------

# ==========================================
# Task 1.4 : Monthly Sales Aggregation
# ==========================================

monthly_sales_df = (

    sales_summary_base_df

    .groupBy(
        "business_month",
        "calendar_quarter",
        "calendar_year"
    )

    .agg(

        # Total Quantity Sold
        sum("quantity")
            .cast("long")
            .alias("total_quantity_sold"),

        # Total Sales Including Tax
        sum("total_including_tax")
            .cast("decimal(18,2)")
            .alias("total_sales"),

        # Total Profit
        # fact_sale does not contain profit
        lit(None)
            .cast("decimal(18,2)")
            .alias("total_profit"),

        # Distinct Orders
        countDistinct("wwi_invoice_id")
            .cast("long")
            .alias("number_of_orders"),

        # Distinct Customers
        countDistinct("customer_key")
            .cast("long")
            .alias("number_of_customers")
    )
)

# COMMAND ----------

monthly_sales_df = (

    monthly_sales_df

    .withColumn(
        "avg_order_value",

        when(
            col("number_of_orders") > 0,

            (
                col("total_sales") /
                col("number_of_orders")
            ).cast("decimal(18,2)")

        ).otherwise(
            lit(0).cast("decimal(18,2)")
        )
    )
)

# COMMAND ----------

monthly_sales_df = monthly_sales_df.select(
    "calendar_year",
    "calendar_quarter",
    "business_month",
    "total_quantity_sold",
    "total_sales",
    "total_profit",
    "number_of_orders",
    "number_of_customers",
    "avg_order_value"
)

# COMMAND ----------

display(monthly_sales_df)

# COMMAND ----------

monthly_sales_df.select(
    "total_quantity_sold",
    "total_sales",
    "number_of_orders",
    "number_of_customers",
    "avg_order_value"
).show()

# COMMAND ----------

monthly_sales_df.filter(
    col("business_month") == "2016-08"
).display()

# COMMAND ----------

# ==========================================
# Task 1.5 : Generate Sales Rollups
# ==========================================

sales_rollup_df = (

    sales_summary_base_df

    .rollup(
        "calendar_year",
        "calendar_quarter",
        "business_month"
    )

    .agg(

        # Total Quantity
        sum("quantity")
            .cast("long")
            .alias("total_quantity_sold"),

        # Total Sales
        sum("total_including_tax")
            .cast("decimal(18,2)")
            .alias("total_sales"),

        # Profit is not available in fact_sale
        lit(None)
            .cast("decimal(18,2)")
            .alias("total_profit"),

        # Distinct Orders
        countDistinct("wwi_invoice_id")
            .cast("long")
            .alias("number_of_orders"),

        # Distinct Customers
        countDistinct("customer_key")
            .cast("long")
            .alias("number_of_customers")
    )
)

# COMMAND ----------

sales_rollup_df = (

    sales_rollup_df

    .withColumn(
        "level",

        when(
            col("business_month").isNotNull() &
            col("calendar_quarter").isNotNull() &
            col("calendar_year").isNotNull(),
            lit("MONTH")
        )

        .when(
            col("business_month").isNull() &
            col("calendar_quarter").isNotNull() &
            col("calendar_year").isNotNull(),
            lit("QUARTER")
        )

        .when(
            col("business_month").isNull() &
            col("calendar_quarter").isNull() &
            col("calendar_year").isNotNull(),
            lit("YEAR")
        )

        .otherwise(
            lit("TOTAL")
        )
    )
)

# COMMAND ----------

sales_rollup_df = (

    sales_rollup_df

    .withColumn(
        "avg_order_value",

        when(
            col("number_of_orders") > 0,

            (
                col("total_sales") /
                col("number_of_orders")
            ).cast("decimal(18,2)")

        ).otherwise(
            lit(0).cast("decimal(18,2)")
        )
    )
)

# COMMAND ----------

sales_rollup_df = (

    sales_rollup_df

    .withColumn(
        "created_timestamp",
        current_timestamp()
    )

    .withColumn(
        "updated_timestamp",
        current_timestamp()
    )
)

# COMMAND ----------

sales_rollup_final_df = sales_rollup_df.select(

    "calendar_year",
    "calendar_quarter",
    "business_month",

    "total_quantity_sold",
    "total_sales",
    "total_profit",

    "number_of_orders",
    "number_of_customers",
    "avg_order_value",

    "level",

    "created_timestamp",
    "updated_timestamp"
)

# COMMAND ----------

display(sales_rollup_final_df.orderBy(
        "calendar_year",
        "calendar_quarter",
        "business_month",
        "level"
    ))

# COMMAND ----------

display(sales_rollup_final_df.groupBy("level").count())

# COMMAND ----------

display(
    sales_rollup_final_df.select(
        "calendar_year",
        "calendar_quarter",
        "business_month",
        "total_quantity_sold",
        "total_sales",
        "number_of_orders",
        "number_of_customers",
        "avg_order_value",
        "level"
    )
)

# COMMAND ----------

sales_rollup_final_df.createOrReplaceTempView(
    "vw_sales_summary_rollup"
)

print("Temporary View Created:")
print("vw_sales_summary_rollup")

# COMMAND ----------

# ==========================================
# Task 1.6 : Delta MERGE
# ==========================================

from delta.tables import DeltaTable

gold_rollup = DeltaTable.forName(
    spark,
    "wwi.gold.sales_summary_rollup"
)

print("Gold Sales Summary Rollup table loaded successfully.")

# COMMAND ----------

# ==========================================
# Task 1.6 : Delta MERGE
# ==========================================

(
    gold_rollup.alias("target")

    .merge(
        sales_rollup_final_df.alias("source"),

        """
        target.calendar_year <=> source.calendar_year
        AND
        target.calendar_quarter <=> source.calendar_quarter
        AND
        target.business_month <=> source.business_month
        AND
        target.level = source.level
        """
    )

    # ======================================
    # Update existing records
    # ======================================

    .whenMatchedUpdate(
        set={
            "total_quantity_sold":
                "source.total_quantity_sold",

            "total_sales":
                "source.total_sales",

            "total_profit":
                "source.total_profit",

            "number_of_orders":
                "source.number_of_orders",

            "number_of_customers":
                "source.number_of_customers",

            "avg_order_value":
                "source.avg_order_value",

            "updated_timestamp":
                "current_timestamp()"
        }
    )

    # ======================================
    # Insert new records
    # ======================================

    .whenNotMatchedInsert(
        values={
            "calendar_year":
                "source.calendar_year",

            "calendar_quarter":
                "source.calendar_quarter",

            "business_month":
                "source.business_month",

            "total_quantity_sold":
                "source.total_quantity_sold",

            "total_sales":
                "source.total_sales",

            "total_profit":
                "source.total_profit",

            "number_of_orders":
                "source.number_of_orders",

            "number_of_customers":
                "source.number_of_customers",

            "avg_order_value":
                "source.avg_order_value",

            "level":
                "source.level",

            "created_timestamp":
                "source.created_timestamp",

            "updated_timestamp":
                "source.updated_timestamp"
        }
    )

    .execute()
)

print("Sales Summary Rollup MERGE Completed Successfully")

# COMMAND ----------

final_rollup_df = spark.table(
    "wwi.gold.sales_summary_rollup"
)

print(
    "Total Gold Rollup Records :",
    final_rollup_df.count()
)

# COMMAND ----------

display(final_rollup_df.groupBy(
    "level"
).count().orderBy(
    "level"
))

# COMMAND ----------

display(final_rollup_df.orderBy(
        col("level")
    ))

# COMMAND ----------

print("==============================================")
print("D9 SALES SUMMARY ROLLUP VALIDATION")
print("==============================================")

print(
    "Source Rollup Records :",
    sales_rollup_final_df.count()
)

print(
    "Gold Rollup Records   :",
    final_rollup_df.count()
)

print("\nRecords by Level:")
display(final_rollup_df.groupBy("level").count())


# COMMAND ----------

