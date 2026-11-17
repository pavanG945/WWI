# WWI Capstone: Medallion Architecture Data Pipeline (Databricks + PySpark)

An end-to-end Medallion architecture (Bronze → Silver → Gold) data
pipeline built on **Databricks** with **PySpark** and **Delta Lake**,
using the Wide World Importers (WWI) sample dataset. Raw operational
data (customers, orders, sales, purchases, stock, employees, etc.) is
ingested monthly from S3, cleaned and validated, and modeled into a
star-schema data warehouse ready for analytics.

## Architecture

```
AWS S3 (monthly CSV drops)
        │  boto3 + pandas -> Spark
        ▼
Bronze  (wwi.bronze.*)      Raw, untouched landing tables.
                             All columns cast to STRING. Incremental
                             loads via business_month + ingestion_timestamp.
        │  PySpark transformations
        ▼
Silver  (wwi.silver.*)      Cleaned, typed, validated staging tables.
                             One notebook per business entity (customer,
                             city, stock item, supplier, employee,
                             movement, payment method, purchase, order,
                             sale, stock holding, transaction, date).
                             Data quality issues logged to
                             wwi.logger.dq_logger with severity and
                             issue-percentage tracking.
        │  PySpark dimensional modeling
        ▼
Gold    (wwi.gold.*)        Star schema: dimension and fact tables,
                             plus a sales summary rollup joining
                             fact_sale with dimension_date.
```

## Why this design

- **Bronze is never modified.** Every column is cast to `STRING` and
  loaded as-is, with `business_month` and `ingestion_timestamp`
  metadata columns added automatically if missing. This preserves a
  permanent, replayable raw history even if downstream transformation
  logic changes or has bugs.
- **Silver separates cleaning from loading.** Each business entity
  (customers, cities, stock items, suppliers, employees, movements,
  payment methods, purchases, orders, sales, stock holdings,
  transactions, transaction types, dates) has its own staging notebook,
  sharing common validation/logging logic from `00_Common_Functions.py`.
- **Data quality is tracked, not just assumed.** A dedicated logger
  table (`wwi.logger.dq_logger`) records every DQ check per pipeline
  run — table, column, check type, severity, and the percentage of
  records failing that check — rather than silently dropping bad rows.
- **Gold is a proper star schema**, not just a flattened table: seven
  dimension tables (City, Customer, Date, Employee, PaymentMethod,
  StockItem, Supplier, TransactionType) and six fact tables (Movement,
  Order, Purchase, Sales, StockHolding, Transaction), plus a summary
  rollup table for fast reporting.

## Repository structure

```
01_Bronze/
├── Bronze_Raw_Data_Ingestion.py   # Full ingestion notebook (S3 -> Bronze)
├── Create_Bronze_Tables.py        # DDL for all Bronze tables + schemas
├── Create_Silver_Tables.py        # DDL for all Silver tables
└── Create_Gold_Tables.py          # DDL for all Gold tables

02_Silver/
├── 00_Common_Functions.py         # Shared DQ logging + validation helpers
├── 01_Customer_Staging.py
├── 02_City_Staging.py
├── 03_Stockitem_Staging.py
├── 04_supplier_staging.py
├── 05_employee_staging.py
├── 06_Movement_Staging.py
├── 07_PaymentMethod_Staging.py
├── 08_Purchase_Staging.py
├── 09_Order_Staging.py
├── 10_Sale_Staging.py
├── 11_StockHolding_staging.py
├── 12_Transaction_Staging.py
├── 13_TransactionType_Staging.py
└── 14_Date_Staging.py

03_Gold/
├── Dimension_City.py
├── Dimension_Customer.py
├── Dimension_Date.py
├── Dimension_Employee.py
├── Dimension_PaymentMethod.py
├── Dimension_StockItem.py
├── Dimension_Supplier.py
├── Dimension_TransactionType.py
├── Fact_Movement.py
├── Fact_Order.py
├── Fact_Purchase.py
├── Fact_Sales.py
├── Fact_StockHolding.py
├── Fact_Transaction.py
└── Summary_Rollup.py              # Sales summary joining fact_sale + dimension_date

3_Loading_Tables.py                # Monthly CSV loader: S3 -> Bronze (standalone)
```

## Tech stack

- **Databricks** (Unity Catalog: `wwi` catalog, `bronze`/`silver`/`logger`/`gold` schemas)
- **PySpark** (DataFrame transformations, window functions, Delta merges)
- **Delta Lake** (`USING DELTA` tables, `DeltaTable` upserts)
- **AWS S3** (monthly source CSV drops, read via `boto3`)
- **Databricks widgets** for pipeline parameterization (e.g. `business_month`)

## Running this pipeline

1. Set up a Databricks Secrets scope for AWS credentials (credentials
   are never hardcoded in this repo):
   ```bash
   databricks secrets create-scope wwi-capstone
   databricks secrets put-secret wwi-capstone aws-access-key
   databricks secrets put-secret wwi-capstone aws-secret-key
   ```
2. Run `01_Bronze/Create_Bronze_Tables.py`, `Create_Silver_Tables.py`,
   and `Create_Gold_Tables.py` once to set up the catalog/schema/table
   structure.
3. Run `01_Bronze/Bronze_Raw_Data_Ingestion.py` (or `3_Loading_Tables.py`
   for a specific month) to load that month's CSVs from S3 into Bronze.
4. Run the `02_Silver/` staging notebooks in order to clean and
   validate data into Silver.
5. Run the `03_Gold/` dimension and fact notebooks to build the star
   schema, then `Summary_Rollup.py` for the reporting layer.

## Notes

- This is a capstone/learning project using the publicly available
  Wide World Importers sample dataset — not production data.
- AWS credentials must be supplied via Databricks Secrets at runtime;
  none are included in this repository.
