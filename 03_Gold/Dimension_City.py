# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Task 1.1 : Notebook Setup
# Gold Dimension - City (SCD Type 2)

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Silver Source
silver_city_table = "wwi.silver.city_staging"

# Gold Target
gold_city_table = "wwi.gold.dimension_city"

print("=" * 60)
print("Gold City Dimension Notebook Started")
print("=" * 60)
print("Silver Table :", silver_city_table)
print("Gold Table   :", gold_city_table)

# COMMAND ----------

# Task 1.2 : Read Silver Data

city_silver_df = (

    spark.table(silver_city_table)

    .select(

        "wwi_city_id",
        "city",
        "state_province",
        "country",
        "continent",
        "sales_territory",
        "region",
        "subregion",
        "location",
        "latest_recorded_population",
        "business_month",
        "created_timestamp",
        "last_updated_timestamp",
        "pipeline_run_id"

    )

)

total_records = city_silver_df.count()

print("=" * 60)
print("Silver Records :", total_records)
print("=" * 60)

if total_records == 0:
    raise Exception("Silver City table is empty.")

city_silver_df.printSchema()

display(city_silver_df)

# COMMAND ----------

spark.table("wwi.silver.city_staging").printSchema()

# COMMAND ----------

spark.sql("SHOW TABLES IN wwi.gold").show(truncate=False)

# COMMAND ----------

# ==========================================
# Task 1.3 : Determine Load Type
# ==========================================

gold_df = spark.table(gold_city_table)

gold_count = gold_df.count()

if gold_count == 0:

    load_type = "INITIAL"

    watermark = None

else:

    load_type = "INCREMENTAL"

    watermark = (

        gold_df

        .agg(
            max("last_updated_timestamp").alias("watermark")
        )

        .collect()[0]["watermark"]

    )

print("=" * 60)
print("Load Type :", load_type)
print("Gold Records :", gold_count)
print("Watermark :", watermark)
print("=" * 60)

# COMMAND ----------

spark.table("wwi.gold.dimension_city").printSchema()

# COMMAND ----------

city_silver_df.printSchema()

# COMMAND ----------

# ==========================================
# Task 1.4 : Initial Load
# ==========================================

if load_type == "INITIAL":

    city_gold_df = (

        city_silver_df

        .withColumn(
            "city_key",
            monotonically_increasing_id() + 1
        )

        .withColumn(
            "wwi_city_id",
            col("wwi_city_id").cast("integer")
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
            "city_key",
            "wwi_city_id",
            "city",
            "state_province",
            "country",
            "continent",
            "sales_territory",
            "region",
            "subregion",
            "location",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"
        )

    )

    city_gold_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_city_table)

    print("=" * 60)
    print("INITIAL LOAD COMPLETED")
    print("=" * 60)
    print("Inserted Records :", city_gold_df.count())

else:

    print("Skipping Initial Load")

# COMMAND ----------

# ==========================================
# Task 1.5 : Incremental Load
# ==========================================

if load_type == "INCREMENTAL":

    incremental_df = (

        city_silver_df

        .filter(
            col("last_updated_timestamp") > lit(watermark)
        )

    )

    incremental_count = incremental_df.count()

    print("=" * 60)
    print("Incremental Records :", incremental_count)
    print("=" * 60)

    if incremental_count == 0:

        print("No Incremental Records Found")

else:

    print("Initial Load Already Completed")

# COMMAND ----------

# ==========================================
# Task 1.6 : Detect New & Changed Records
# ==========================================

gold_current_df = (

    spark.table(gold_city_table)

    .filter(col("is_current") == 1)

)

# COMMAND ----------

comparison_df = (

    incremental_df.alias("s")

    .join(

        gold_current_df.alias("g"),

        col("s.wwi_city_id") == col("g.wwi_city_id"),

        "left"

    )

)

# COMMAND ----------

new_records_df = (

    comparison_df

    .filter(col("g.wwi_city_id").isNull())

    .select("s.*")

)

print("New Records :", new_records_df.count())

# COMMAND ----------

# ==========================================
# Task 1.6 : Detect Changed Records
# ==========================================

changed_records_df = (

    comparison_df

    .filter(

        col("g.wwi_city_id").isNotNull()

        & (

            (col("s.city") != col("g.city")) |

            (col("s.state_province") != col("g.state_province")) |

            (col("s.country") != col("g.country")) |

            (col("s.subregion") != col("g.subregion")) |

            (col("s.location") != col("g.location"))

        )

    )

    .select("s.*")

)

print("Changed Records :", changed_records_df.count())

# COMMAND ----------

from delta.tables import DeltaTable

gold_delta = DeltaTable.forName(
    spark,
    gold_city_table
)

gold_delta.alias("g").merge(

    changed_records_df.alias("s"),

    "g.wwi_city_id = s.wwi_city_id AND g.is_current = 1"

).whenMatchedUpdate(

    set={

        "is_current": "0",

        "valid_to": "s.last_updated_timestamp",

        "updated_timestamp": "current_timestamp()"

    }

).execute()

print("Existing records expired successfully.")

# COMMAND ----------

# ==========================================
# Task 1.7 : Insert New SCD2 Versions
# ==========================================

# Combine New and Changed Records
new_version_df = (

    new_records_df

    .unionByName(changed_records_df)

)

records_to_insert = new_version_df.count()

print("Records to Insert :", records_to_insert)

if records_to_insert > 0:

    # Get Current Maximum Surrogate Key
    max_city_key = (

        spark.table(gold_city_table)

        .agg(max("city_key").alias("max_key"))

        .collect()[0]["max_key"]

    )

    if max_city_key is None:
        max_city_key = 0

    # Generate New Surrogate Keys
    windowSpec = Window.partitionBy(lit(1)).orderBy("wwi_city_id")

    city_new_version_df = (

        new_version_df

        .withColumn(
            "city_key",
            row_number().over(windowSpec) + max_city_key
        )

        # SCD2 Columns
        .withColumn("valid_from", col("last_updated_timestamp"))

        .withColumn(
            "valid_to",
            to_timestamp(lit("9999-12-31 23:59:59"))
        )

        .withColumn("is_current", lit(1))

        .withColumn("updated_timestamp", current_timestamp())

        .select(

            "city_key",
            col("wwi_city_id").cast("integer").alias("wwi_city_id"),
            "city",
            "state_province",
            "country",
            "continent",
            "sales_territory",
            "region",
            "subregion",
            "location",
            "valid_from",
            "valid_to",
            "is_current",
            "created_timestamp",
            "updated_timestamp",
            "last_updated_timestamp"

        )

    )

    # Count before writing (Spark Connect issue with post-write counts)
    final_insert_count = city_new_version_df.count()
    
    city_new_version_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_city_table)

    print("=" * 60)
    print("New SCD2 Versions Inserted Successfully")
    print("=" * 60)
    print("Inserted Records :", final_insert_count)

else:

    print("=" * 60)
    print("No New or Changed Records Found")
    print("=" * 60)

# COMMAND ----------

# ==========================================
# Task 1.8 : Validate Gold Dimension
# ==========================================

gold_city_df = spark.table(gold_city_table)

# Total Records
total_records = gold_city_df.count()

# Active Records
active_records = gold_city_df.filter(
    col("is_current") == 1
).count()

# Historical Records
historical_records = gold_city_df.filter(
    col("is_current") == 0
).count()

# Business Keys Having More Than One Active Record
multiple_active_df = (

    gold_city_df

    .filter(col("is_current") == 1)

    .groupBy("wwi_city_id")

    .count()

    .filter(col("count") > 1)

)

multiple_active_count = multiple_active_df.count()

# Business Keys Having Multiple Versions
multiple_versions_df = (

    gold_city_df

    .groupBy("wwi_city_id")

    .count()

    .filter(col("count") > 1)

)

multiple_versions_count = multiple_versions_df.count()

print("=" * 60)
print("Gold Dimension City Validation")
print("=" * 60)

print("Total Records                 :", total_records)
print("Active Records                :", active_records)
print("Historical Records            :", historical_records)
print("Multiple Active Records       :", multiple_active_count)
print("Business Keys with Versions   :", multiple_versions_count)

print("=" * 60)

display(multiple_versions_df)

# COMMAND ----------

spark.table(gold_city_table).count()


# COMMAND ----------

display(spark.table(gold_city_table))

# COMMAND ----------

