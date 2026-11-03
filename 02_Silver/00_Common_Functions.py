# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Common Functions for Silver Layer

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

import uuid
from datetime import datetime

# COMMAND ----------

# Verify Logger Table
def verify_logger_table(logger_table):

    if spark.catalog.tableExists(logger_table):

        print(f"{logger_table} exists.")

    else:

        raise Exception(f"{logger_table} does not exist.")

# COMMAND ----------

# DQ Logger Function
from pyspark.sql import Row
from datetime import datetime
import uuid

def log_dq(
    table_name,
    column_name,
    dq_type,
    dq_description,
    check_category,
    check_name,
    severity,
    total_records,
    failed_records,
    pipeline_run_id,
    dq_logger_table
):

    issue_percentage = (
        (failed_records / total_records) * 100
        if total_records > 0 else 0
    )

    dq_pass_rate = 100 - issue_percentage

    dq_row = [Row(

        dq_id=str(uuid.uuid4()),

        pipeline_run_id=pipeline_run_id,

        table_name=table_name,

        column_name=column_name,

        dq_type=dq_type,

        dq_description=dq_description,

        check_category=check_category,

        check_name=check_name,

        severity=severity,

        total_records=int(total_records),

        failed_records=int(failed_records),

        issue_percentage=float(issue_percentage),

        dq_pass_rate=float(dq_pass_rate),

        check_timestamp=datetime.now()

    )]

    dq_df = spark.createDataFrame(dq_row)

    dq_df.write.mode("append").saveAsTable(dq_logger_table)

# COMMAND ----------

