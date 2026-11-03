# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.city_staging (
# MAGIC     wwi_city_id BIGINT,
# MAGIC     city STRING,
# MAGIC     state_province STRING,
# MAGIC     country STRING,
# MAGIC     continent STRING,
# MAGIC     sales_territory STRING,
# MAGIC     region STRING,
# MAGIC     subregion STRING,
# MAGIC     location STRING,
# MAGIC     latest_recorded_population BIGINT,
# MAGIC     business_month STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     pipeline_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_records
# MAGIC FROM wwi.silver.city_staging;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.customer_staging (
# MAGIC     wwi_customer_id INT,
# MAGIC     customer STRING,
# MAGIC     bill_to_customer INT,
# MAGIC     category STRING,
# MAGIC     buying_group STRING,
# MAGIC     primary_contact INT,
# MAGIC     postal_code STRING,
# MAGIC     business_month STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     pipeline_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_records
# MAGIC FROM wwi.silver.city_staging;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.employee_staging (
# MAGIC     wwi_employee_id INT,
# MAGIC     employee STRING,
# MAGIC     preferred_name STRING,
# MAGIC     is_salesperson BOOLEAN,
# MAGIC     photo STRING,
# MAGIC     business_month STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     pipeline_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.movement_staging (
# MAGIC     wwi_stock_item_transaction_id BIGINT,
# MAGIC     wwi_stock_item_id BIGINT,
# MAGIC     wwi_transaction_type_id BIGINT,
# MAGIC     wwi_customer_id BIGINT,
# MAGIC     wwi_invoice_id BIGINT,
# MAGIC     wwi_supplier_id BIGINT,
# MAGIC     wwi_purchase_order_id BIGINT,
# MAGIC     date_key INT,
# MAGIC     quantity INT,
# MAGIC     business_month STRING,
# MAGIC     last_modified_when TIMESTAMP,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     pipeline_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.order_staging (
# MAGIC     wwi_order_id BIGINT,
# MAGIC     wwi_order_line_id BIGINT,
# MAGIC     wwi_backorder_id BIGINT,
# MAGIC     wwi_customer_id BIGINT,
# MAGIC     wwi_city_id BIGINT,
# MAGIC     wwi_stock_item_id BIGINT,
# MAGIC     wwi_salesperson_id BIGINT,
# MAGIC     wwi_picker_id BIGINT,
# MAGIC     wwi_contact_person_id BIGINT,
# MAGIC     order_date_key INT,
# MAGIC     picked_date_key INT,
# MAGIC     description STRING,
# MAGIC     package STRING,
# MAGIC     quantity BIGINT,
# MAGIC     unit_price DECIMAL,
# MAGIC     tax_rate DECIMAL,
# MAGIC     total_excluding_tax DECIMAL,
# MAGIC     tax_amount DOUBLE,
# MAGIC     total_including_tax DOUBLE,
# MAGIC     business_month STRING,
# MAGIC     last_modified_when TIMESTAMP,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     pipeline_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.paymentmethod_staging (
# MAGIC     wwi_payment_method_id BIGINT,
# MAGIC     payment_method STRING,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     business_month STRING,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_edited_by BIGINT,
# MAGIC     pipeline_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.purchase_staging (
# MAGIC     wwi_purchase_order_id BIGINT,
# MAGIC     wwi_stock_item_id BIGINT,
# MAGIC     ordered_outers INT,
# MAGIC     ordered_quantity INT,
# MAGIC     received_outers INT,
# MAGIC     is_order_line_finalized STRING,
# MAGIC     wwi_supplier_id BIGINT,
# MAGIC     package STRING,
# MAGIC     date_key TIMESTAMP,
# MAGIC     last_modified_date TIMESTAMP,
# MAGIC     business_month STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     pipeline_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.sale_staging (
# MAGIC     wwi_invoice_id BIGINT,
# MAGIC     wwi_stock_item_id BIGINT,
# MAGIC     wwi_customer_id BIGINT,
# MAGIC     wwi_bill_to_customer_id BIGINT,
# MAGIC     wwi_city_id BIGINT,
# MAGIC     wwi_salesperson_id BIGINT,
# MAGIC     invoice_date_key TIMESTAMP,
# MAGIC     delivery_date_key TIMESTAMP,
# MAGIC     description STRING,
# MAGIC     package STRING,
# MAGIC     quantity INT,
# MAGIC     unit_price DECIMAL,
# MAGIC     tax_rate DECIMAL,
# MAGIC     total_excluding_tax DECIMAL,
# MAGIC     tax_amount DECIMAL,
# MAGIC     profit DECIMAL,
# MAGIC     total_including_tax DECIMAL,
# MAGIC     total_dry_items INT,
# MAGIC     total_chiller_items INT,
# MAGIC     last_modified_date TIMESTAMP,
# MAGIC     business_month STRING,
# MAGIC     source_system STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     pipeline_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.stockholding_staging (
# MAGIC     wwi_stock_item_id BIGINT,
# MAGIC     quantity_on_hand INT,
# MAGIC     bin_location STRING,
# MAGIC     last_stocktake_quantity INT,
# MAGIC     last_cost_price DECIMAL,
# MAGIC     reorder_level INT,
# MAGIC     target_stock_level INT,
# MAGIC     stock_item_name STRING,
# MAGIC     last_modified_date TIMESTAMP,
# MAGIC     source_system STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP,
# MAGIC     business_month STRING,
# MAGIC     pipeline_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.stockitem_staging (
# MAGIC     stock_item_id BIGINT,
# MAGIC     stock_item_name STRING,
# MAGIC     supplier_id INT,
# MAGIC     color_id INT,
# MAGIC     color_name STRING,
# MAGIC     unit_package_id INT,
# MAGIC     unit_package_name STRING,
# MAGIC     brand STRING,
# MAGIC     unit_price DOUBLE,
# MAGIC     tax_rate DOUBLE,
# MAGIC     lead_time_days INT,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     pipeline_run_id STRING,
# MAGIC     business_month STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.supplier_staging (
# MAGIC     wwi_supplier_id INT,
# MAGIC     supplier_name STRING,
# MAGIC     category STRING,
# MAGIC     website_url STRING,
# MAGIC     primary_contact STRING,
# MAGIC     supplier_reference STRING,
# MAGIC     payment_days INT,
# MAGIC     postal_code STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     pipeline_run_id STRING,
# MAGIC     business_month STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.transaction_staging (
# MAGIC     transactiondate TIMESTAMP,
# MAGIC     wwicustomertransactionid INT,
# MAGIC     wwisuppliertransactionid INT,
# MAGIC     wwiinvoiceid DOUBLE,
# MAGIC     wwipurchaseorderid INT,
# MAGIC     supplierinvoicenumber BIGINT,
# MAGIC     totalexcludingtax DOUBLE,
# MAGIC     taxamount DOUBLE,
# MAGIC     totalincludingtax DOUBLE,
# MAGIC     outstandingbalance DOUBLE,
# MAGIC     isfinalized BOOLEAN,
# MAGIC     wwicustomerid INT,
# MAGIC     wwibilltocustomerid INT,
# MAGIC     wwisupplierid INT,
# MAGIC     wwitransactiontypeid INT,
# MAGIC     wwipaymentmethodid DOUBLE,
# MAGIC     business_month STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     suppliername STRING,
# MAGIC     pipeline_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.transactiontype_staging (
# MAGIC     transaction_type_id BIGINT,
# MAGIC     transaction_type_name STRING,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP,
# MAGIC     pipeline_run_id STRING,
# MAGIC     business_month STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.silver.date_staging (
# MAGIC     id INT,
# MAGIC     date_key INT,
# MAGIC     date_value DATE,
# MAGIC     day_number INT,
# MAGIC     day_of_year INT,
# MAGIC     day_of_month INT,
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
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

