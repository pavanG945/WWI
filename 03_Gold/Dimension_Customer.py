# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ==========================================
# Task 1.1 : Notebook Setup
# Gold Dimension - Customer (SCD Type 2)
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Silver Source
silver_customer_table = "wwi.silver.customer_staging"

# Gold Target
gold_customer_table = "wwi.gold.dimension_customer"

print("=" * 60)
print("Gold Customer Dimension Notebook Started")
print("=" * 60)
print("Silver Source :", silver_customer_table)
print("Gold Target   :", gold_customer_table)

# COMMAND ----------

# ==========================================
# Task 1.2 : Read Silver Customer Data
# ==========================================

customer_silver_df = spark.table(silver_customer_table)

customer_silver_df.printSchema()

total_records = customer_silver_df.count()

print("=" * 60)
print("Silver Customer Records :", total_records)
print("=" * 60)

if total_records == 0:
    raise Exception("Customer Silver table is empty.")

display(customer_silver_df)

# COMMAND ----------

# ==========================================
# Task 1.3 : Determine Load Type
# ==========================================

gold_customer_df = spark.table(gold_customer_table)

gold_count = gold_customer_df.count()

if gold_count == 0:

    load_type = "INITIAL"
    watermark = None

else:

    load_type = "INCREMENTAL"

    watermark = (

        gold_customer_df

        .agg(
            max("last_updated_timestamp").alias("watermark")
        )

        .collect()[0]["watermark"]

    )

print("=" * 60)
print("Load Type    :", load_type)
print("Gold Records :", gold_count)
print("Watermark    :", watermark)
print("=" * 60)

# COMMAND ----------

spark.table("wwi.silver.customer_staging").printSchema()

# COMMAND ----------

spark.table("wwi.gold.dimension_customer").printSchema()

# COMMAND ----------

# ==========================================
# Task 1.4 : Initial Load
# ==========================================

if load_type == "INITIAL":

    customer_gold_df = (

        customer_silver_df

        .withColumn(
            "customer_key",
            monotonically_increasing_id() + 1
        )

        .withColumn(
            "postal_code",
            col("postal_code").cast("integer")
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

            "customer_key",
            "wwi_customer_id",
            "customer",
            "bill_to_customer",
            "category",
            "buying_group",
            "primary_contact",
            "postal_code",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp",
            "business_month"

        )

    )

    customer_gold_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_customer_table)

    print("=" * 60)
    print("INITIAL LOAD COMPLETED")
    print("=" * 60)
    print("Inserted Records :", customer_gold_df.count())

else:

    print("Skipping Initial Load")

# COMMAND ----------

# ==========================================
# Task 1.5 : Incremental Load
# ==========================================

if load_type == "INCREMENTAL":

    incremental_df = (

        customer_silver_df

        .filter(
            col("last_updated_timestamp") > lit(watermark)
        )

    )

    incremental_count = incremental_df.count()

    print("=" * 60)
    print("Incremental Records :", incremental_count)
    print("=" * 60)

    if incremental_count == 0:

        raise Exception("No Incremental Records Found.")

else:

    print("Initial Load Already Completed")

# COMMAND ----------

# ==========================================
# Task 1.6 : Detect New & Changed Records
# ==========================================

gold_current_df = (

    spark.table(gold_customer_table)

    .filter(col("is_current") == 1)

)

comparison_df = (

    incremental_df.alias("s")

    .join(

        gold_current_df.alias("g"),

        col("s.wwi_customer_id") == col("g.wwi_customer_id"),

        "left"

    )

)

# New Records
new_records_df = (

    comparison_df

    .filter(col("g.wwi_customer_id").isNull())

    .select("s.*")

)

# Changed Records
changed_records_df = (

    comparison_df

    .filter(

        col("g.wwi_customer_id").isNotNull()

        & (

            (col("s.customer") != col("g.customer")) |

            (col("s.bill_to_customer") != col("g.bill_to_customer")) |

            (col("s.category") != col("g.category")) |

            (col("s.buying_group") != col("g.buying_group")) |

            (col("s.primary_contact") != col("g.primary_contact")) |

            (col("s.postal_code").cast("integer") != col("g.postal_code"))

        )

    )

    .select("s.*")

)

print("New Records     :", new_records_df.count())
print("Changed Records :", changed_records_df.count())

# COMMAND ----------

gold_delta = DeltaTable.forName(spark, gold_customer_table)

gold_delta.alias("g").merge(

    changed_records_df.alias("s"),

    "g.wwi_customer_id = s.wwi_customer_id AND g.is_current = 1"

).whenMatchedUpdate(

    set={

        "is_current": "0",

        "valid_to": "s.last_updated_timestamp",

        "updated_timestamp": "current_timestamp()"

    }

).execute()

print("Existing active records expired.")

# COMMAND ----------

# ==========================================
# Task 1.7 : Insert New SCD2 Versions
# ==========================================

customer_new_version_df = (

    new_records_df

    .unionByName(changed_records_df)

)

records_to_insert = customer_new_version_df.count()

print("Records to Insert :", records_to_insert)

if records_to_insert > 0:

    # Get Current Maximum Surrogate Key
    max_customer_key = (

        spark.table(gold_customer_table)

        .agg(max("customer_key").alias("max_key"))

        .collect()[0]["max_key"]

    )

    if max_customer_key is None:
        max_customer_key = 0

    windowSpec = Window.partitionBy(lit(1)).orderBy("wwi_customer_id")

    customer_new_version_df = (

        customer_new_version_df

        .withColumn(
            "customer_key",
            row_number().over(windowSpec) + max_customer_key
        )

        .withColumn(
            "postal_code",
            col("postal_code").cast("integer")
        )

        # SCD2 Columns
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

            "customer_key",
            "wwi_customer_id",
            "customer",
            "bill_to_customer",
            "category",
            "buying_group",
            "primary_contact",
            "postal_code",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp",
            "business_month"

        )

    )

    # Count before writing (Spark Connect issue with post-write counts)
    final_insert_count = customer_new_version_df.count()
    
    customer_new_version_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_customer_table)

    print("=" * 60)
    print("New Customer Versions Inserted Successfully")
    print("=" * 60)
    print("Inserted Records :", final_insert_count)

else:

    print("=" * 60)
    print("No New or Changed Records Found")
    print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 1.8 : Validate Gold Customer Dimension
# ==========================================

gold_customer_df = spark.table(gold_customer_table)

# Total Records
total_records = gold_customer_df.count()

# Active Records
active_records = gold_customer_df.filter(
    col("is_current") == 1
).count()

# Historical Records
historical_records = gold_customer_df.filter(
    col("is_current") == 0
).count()

# Multiple Active Records
multiple_active_df = (

    gold_customer_df

    .filter(col("is_current") == 1)

    .groupBy("wwi_customer_id")

    .count()

    .filter(col("count") > 1)

)

multiple_active_count = multiple_active_df.count()

# Customers with Multiple Versions
multiple_versions_df = (

    gold_customer_df

    .groupBy("wwi_customer_id")

    .count()

    .filter(col("count") > 1)

)

multiple_versions_count = multiple_versions_df.count()

print("=" * 60)
print("Gold Customer Dimension Validation")
print("=" * 60)

print("Total Records               :", total_records)
print("Active Records              :", active_records)
print("Historical Records          :", historical_records)
print("Multiple Active Records     :", multiple_active_count)
print("Customers with Versions     :", multiple_versions_count)

print("=" * 60)

display(multiple_versions_df)

# COMMAND ----------

display(spark.table(gold_customer_table))

# COMMAND ----------

