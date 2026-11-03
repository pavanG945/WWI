# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC USE CATALOG wwi;
# MAGIC USE SCHEMA logger;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS wwi.logger.dq_logger
# MAGIC (
# MAGIC     dq_id STRING,
# MAGIC     pipeline_run_id STRING,
# MAGIC     table_name STRING,
# MAGIC     column_name STRING,
# MAGIC     dq_type STRING,
# MAGIC     dq_description STRING,
# MAGIC     check_category STRING,
# MAGIC     check_name STRING,
# MAGIC     severity STRING,
# MAGIC     total_records BIGINT,
# MAGIC     failed_records BIGINT,
# MAGIC     issue_percentage DOUBLE,
# MAGIC     dq_pass_rate DOUBLE,
# MAGIC     check_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.logger.dq_logger;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.dimension_city
# MAGIC (
# MAGIC     city_key BIGINT,
# MAGIC     wwi_city_id INT,
# MAGIC     city STRING,
# MAGIC     state_province STRING,
# MAGIC     country STRING,
# MAGIC     continent STRING,
# MAGIC     sales_territory STRING,
# MAGIC     region STRING,
# MAGIC     subregion STRING,
# MAGIC     location STRING,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current INT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.dimension_customer
# MAGIC (
# MAGIC     customer_key BIGINT,
# MAGIC     wwi_customer_id INT,
# MAGIC     customer STRING,
# MAGIC     bill_to_customer INT,
# MAGIC     category STRING,
# MAGIC     buying_group STRING,
# MAGIC     primary_contact INT,
# MAGIC     postal_code INT,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current INT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     business_month STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS wwi.gold.dimension_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.dimension_date
# MAGIC (
# MAGIC     id INT,
# MAGIC     date_key INT,
# MAGIC     date_value DATE,
# MAGIC     day_number INT,
# MAGIC     day STRING,
# MAGIC     month_name STRING,
# MAGIC     month INT,
# MAGIC     short_month STRING,
# MAGIC     calendar_month_number INT,
# MAGIC     calendar_month_label STRING,
# MAGIC     calendar_year INT,
# MAGIC     calendar_year_label STRING,
# MAGIC     fiscal_month_number INT,
# MAGIC     fiscal_month_label STRING,
# MAGIC     fiscal_year INT,
# MAGIC     fiscal_year_label STRING,
# MAGIC     is_weekend STRING,
# MAGIC     business_month STRING,
# MAGIC     iso_week_number INT,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current INT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.dimension_employee
# MAGIC (
# MAGIC     employee_key BIGINT,
# MAGIC     wwi_employee_id INT,
# MAGIC     employee STRING,
# MAGIC     preferred_name STRING,
# MAGIC     phone_number STRING,
# MAGIC     is_salesperson BOOLEAN,
# MAGIC     photo STRING,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current INT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.dimension_payment_method
# MAGIC (
# MAGIC     payment_method_key BIGINT,
# MAGIC     wwi_payment_method_id BIGINT,
# MAGIC     payment_method STRING,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current INT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.dimension_stock_item
# MAGIC (
# MAGIC     stock_item_key BIGINT,
# MAGIC     wwi_stock_item_id BIGINT,
# MAGIC     stock_item STRING,
# MAGIC     color_id INT,
# MAGIC     color_name STRING,
# MAGIC     unit_package_id INT,
# MAGIC     unit_package_name STRING,
# MAGIC     brand STRING,
# MAGIC     lead_time_days INT,
# MAGIC     tax_rate DOUBLE,
# MAGIC     unit_price DOUBLE,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current INT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.dimension_supplier
# MAGIC (
# MAGIC     supplier_key BIGINT,
# MAGIC     wwi_supplier_id BIGINT,
# MAGIC     supplier_name STRING,
# MAGIC     category STRING,
# MAGIC     primary_contact STRING,
# MAGIC     supplier_reference STRING,
# MAGIC     payment_days INT,
# MAGIC     postal_code STRING,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current INT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.dimension_transaction_type
# MAGIC (
# MAGIC     transaction_type_key BIGINT,
# MAGIC     wwi_transaction_type_id INT,
# MAGIC     transaction_type STRING,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current INT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.fact_movement
# MAGIC (
# MAGIC     movement_key BIGINT,
# MAGIC     date_key INT,
# MAGIC     stock_item_key BIGINT,
# MAGIC     customer_key BIGINT,
# MAGIC     supplier_key BIGINT,
# MAGIC     transaction_type_key BIGINT,
# MAGIC     wwi_stock_item_transaction_id BIGINT,
# MAGIC     wwi_invoice_id BIGINT,
# MAGIC     wwi_purchase_order_id BIGINT,
# MAGIC     quantity INT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.fact_order
# MAGIC (
# MAGIC     order_key BIGINT,
# MAGIC     city_key BIGINT,
# MAGIC     customer_key BIGINT,
# MAGIC     stock_item_key BIGINT,
# MAGIC     order_date_key BIGINT,
# MAGIC     picked_date_key BIGINT,
# MAGIC     salesperson_key BIGINT,
# MAGIC     picker_key BIGINT,
# MAGIC     wwi_order_id BIGINT,
# MAGIC     wwi_backorder_id BIGINT,
# MAGIC     description STRING,
# MAGIC     package STRING,
# MAGIC     quantity BIGINT,
# MAGIC     tax_rate DECIMAL(18,2),
# MAGIC     total_excluding_tax DECIMAL(18,2),
# MAGIC     tax_amount DOUBLE,
# MAGIC     total_including_tax DECIMAL(18,2),
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.fact_purchase
# MAGIC (
# MAGIC     purchase_key BIGINT,
# MAGIC     date_key INT,
# MAGIC     supplier_key BIGINT,
# MAGIC     stock_item_key BIGINT,
# MAGIC     wwi_purchase_order_id BIGINT,
# MAGIC     ordered_outers INT,
# MAGIC     ordered_quantity INT,
# MAGIC     received_outers INT,
# MAGIC     package STRING,
# MAGIC     is_order_finalized STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.fact_sale
# MAGIC (
# MAGIC     sale_key BIGINT,
# MAGIC     city_key BIGINT,
# MAGIC     customer_key BIGINT,
# MAGIC     bill_to_customer_key BIGINT,
# MAGIC     stock_item_key BIGINT,
# MAGIC     invoice_date_key INT,
# MAGIC     delivery_date_key INT,
# MAGIC     salesperson_key BIGINT,
# MAGIC     wwi_invoice_id BIGINT,
# MAGIC     description STRING,
# MAGIC     package STRING,
# MAGIC     quantity INT,
# MAGIC     unit_price DECIMAL(18,2),
# MAGIC     tax_rate DECIMAL(18,2),
# MAGIC     total_excluding_tax DECIMAL(18,2),
# MAGIC     tax_amount DECIMAL(18,2),
# MAGIC     total_including_tax DECIMAL(18,2),
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.fact_stock_holding
# MAGIC (
# MAGIC     stock_holding_key BIGINT,
# MAGIC     stock_item_key BIGINT,
# MAGIC     quantity_on_hand BIGINT,
# MAGIC     bin_location STRING,
# MAGIC     last_stocktake_quantity BIGINT,
# MAGIC     last_cost_price DOUBLE,
# MAGIC     reorder_level BIGINT,
# MAGIC     target_stock_level BIGINT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.fact_transaction
# MAGIC (
# MAGIC     transaction_date_key INT,
# MAGIC     customer_key BIGINT,
# MAGIC     bill_to_customer_key BIGINT,
# MAGIC     supplier_key BIGINT,
# MAGIC     transaction_type_key BIGINT,
# MAGIC     payment_method_key BIGINT,
# MAGIC     wwi_customer_transaction_id INT,
# MAGIC     wwi_supplier_transaction_id INT,
# MAGIC     wwi_invoice_id DOUBLE,
# MAGIC     wwi_purchase_order_id INT,
# MAGIC     supplier_invoice_number BIGINT,
# MAGIC     total_excluding_tax DECIMAL(18,2),
# MAGIC     tax_amount DECIMAL(18,2),
# MAGIC     total_including_tax DECIMAL(18,2),
# MAGIC     outstanding_balance DECIMAL(18,2),
# MAGIC     is_finalized BOOLEAN,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.gold.sales_summary_rollup
# MAGIC (
# MAGIC     calendar_year INT,
# MAGIC     calendar_quarter INT,
# MAGIC     business_month STRING,
# MAGIC     total_quantity_sold BIGINT,
# MAGIC     total_sales DECIMAL(18,2),
# MAGIC     total_profit DECIMAL(18,2),
# MAGIC     number_of_orders BIGINT,
# MAGIC     number_of_customers BIGINT,
# MAGIC     avg_order_value DECIMAL(18,2),
# MAGIC     level STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN wwi.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE wwi.gold.sales_summary_rollup;

# COMMAND ----------

