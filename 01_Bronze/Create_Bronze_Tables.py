# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC USE CATALOG wwi;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS logger;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN wwi;

# COMMAND ----------

spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.application_cities (
# MAGIC     business_month STRING,
# MAGIC     cityid STRING,
# MAGIC     cityname STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     latestrecordedpopulation STRING,
# MAGIC     location STRING,
# MAGIC     stateprovinceid STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.application_countries (
# MAGIC     border STRING,
# MAGIC     business_month STRING,
# MAGIC     continent STRING,
# MAGIC     countryid STRING,
# MAGIC     countryname STRING,
# MAGIC     countrytype STRING,
# MAGIC     formalname STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     isoalpha3code STRING,
# MAGIC     isonumericcode STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     latestrecordedpopulation STRING,
# MAGIC     region STRING,
# MAGIC     subregion STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.application_deliverymethods (
# MAGIC     business_month STRING,
# MAGIC     deliverymethodid STRING,
# MAGIC     deliverymethodname STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.application_paymentmethods (
# MAGIC     business_month STRING,
# MAGIC     ingested_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     paymentmethodid STRING,
# MAGIC     paymentmethodname STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.application_people (
# MAGIC     business_month STRING,
# MAGIC     customfields STRING,
# MAGIC     emailaddress STRING,
# MAGIC     faxnumber STRING,
# MAGIC     fullname STRING,
# MAGIC     hashedpassword STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     isemployee STRING,
# MAGIC     isexternallogonprovider STRING,
# MAGIC     ispermittedtologon STRING,
# MAGIC     issalesperson STRING,
# MAGIC     issystemuser STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     logonname STRING,
# MAGIC     otherlanguages STRING,
# MAGIC     personid STRING,
# MAGIC     phonenumber STRING,
# MAGIC     photo STRING,
# MAGIC     preferredname STRING,
# MAGIC     searchname STRING,
# MAGIC     userpreferences STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.application_stateprovinces (
# MAGIC     border STRING,
# MAGIC     business_month STRING,
# MAGIC     countryid STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     latestrecordedpopulation STRING,
# MAGIC     salesterritory STRING,
# MAGIC     stateprovincecode STRING,
# MAGIC     stateprovinceid STRING,
# MAGIC     stateprovincename STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.application_systemparameters (
# MAGIC     applicationsettings STRING,
# MAGIC     business_month STRING,
# MAGIC     deliveryaddressline1 STRING,
# MAGIC     deliveryaddressline2 STRING,
# MAGIC     deliverycityid STRING,
# MAGIC     deliverylocation STRING,
# MAGIC     deliverypostalcode STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     postaladdressline1 STRING,
# MAGIC     postaladdressline2 STRING,
# MAGIC     postalcityid STRING,
# MAGIC     postalpostalcode STRING,
# MAGIC     systemparameterid STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.application_transactiontypes (
# MAGIC     business_month STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     transactiontypename STRING,
# MAGIC     transactiontypeid STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.purchasing_purchaseorderlines (
# MAGIC     business_month STRING,
# MAGIC     description STRING,
# MAGIC     expectedunitpriceperouter STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     isorderlinefinalized STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     lastreceiptdate STRING,
# MAGIC     orderedouters STRING,
# MAGIC     packagetypeid STRING,
# MAGIC     purchaseorderid STRING,
# MAGIC     purchaseorderlineid STRING,
# MAGIC     receivedouters STRING,
# MAGIC     stockitemid STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.purchasing_purchaseorders (
# MAGIC     business_month STRING,
# MAGIC     comments STRING,
# MAGIC     contactpersonid STRING,
# MAGIC     deliverymethodid STRING,
# MAGIC     expecteddeliverydate STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     internalcomments STRING,
# MAGIC     isorderfinalized STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     orderdate STRING,
# MAGIC     purchaseorderid STRING,
# MAGIC     supplierid STRING,
# MAGIC     supplierreference STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.purchasing_suppliercategories(
# MAGIC     business_month STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     suppliercategoryid STRING,
# MAGIC     suppliercategoryname STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.purchasing_suppliers (
# MAGIC     alternatecontactpersonid STRING,
# MAGIC     bankaccountbranch STRING,
# MAGIC     bankaccountcode STRING,
# MAGIC     bankaccountname STRING,
# MAGIC     bankaccountnumber STRING,
# MAGIC     bankinternationalcode STRING,
# MAGIC     business_month STRING,
# MAGIC     deliveryaddressline1 STRING,
# MAGIC     deliveryaddressline2 STRING,
# MAGIC     deliverycityid STRING,
# MAGIC     deliverylocation STRING,
# MAGIC     deliverymethodid STRING,
# MAGIC     deliverypostalcode STRING,
# MAGIC     faxnumber STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     internalcomments STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     paymentdays STRING,
# MAGIC     phonenumber STRING,
# MAGIC     postaladdressline1 STRING,
# MAGIC     postaladdressline2 STRING,
# MAGIC     postalcityid STRING,
# MAGIC     postalpostalcode STRING,
# MAGIC     primarycontactpersonid STRING,
# MAGIC     suppliercategoryid STRING,
# MAGIC     supplierid STRING,
# MAGIC     suppliername STRING,
# MAGIC     supplierreference STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING,
# MAGIC     websiteurl STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.purchasing_suppliertransactions (
# MAGIC     amountexcludingtax STRING,
# MAGIC     business_month STRING,
# MAGIC     finalizationdate STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     isfinalized STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     outstandingbalance STRING,
# MAGIC     paymentmethodid STRING,
# MAGIC     purchaseorderid STRING,
# MAGIC     supplierid STRING,
# MAGIC     supplierinvoicenumber STRING,
# MAGIC     suppliertransactionid STRING,
# MAGIC     taxamount STRING,
# MAGIC     transactionamount STRING,
# MAGIC     transactiondate STRING,
# MAGIC     transactiontypeid STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.sales_buyinggroups (
# MAGIC     business_month STRING,
# MAGIC     buyinggroupid STRING,
# MAGIC     buyinggroupname STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.sales_customercategories (
# MAGIC     business_month STRING,
# MAGIC     customercategoryid STRING,
# MAGIC     customercategoryname STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.sales_customers (
# MAGIC     accountopeneddate STRING,
# MAGIC     alternatecontactpersonid STRING,
# MAGIC     billtocustomerid STRING,
# MAGIC     business_month STRING,
# MAGIC     buyinggroupid STRING,
# MAGIC     creditlimit STRING,
# MAGIC     customercategoryid STRING,
# MAGIC     customerid STRING,
# MAGIC     customername STRING,
# MAGIC     deliveryaddressline1 STRING,
# MAGIC     deliveryaddressline2 STRING,
# MAGIC     deliverycityid STRING,
# MAGIC     deliverylocation STRING,
# MAGIC     deliverymethodid STRING,
# MAGIC     deliverypostalcode STRING,
# MAGIC     deliveryrun STRING,
# MAGIC     faxnumber STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     isoncredithold STRING,
# MAGIC     isstatementsent STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     paymentdays STRING,
# MAGIC     phonenumber STRING,
# MAGIC     postaladdressline1 STRING,
# MAGIC     postaladdressline2 STRING,
# MAGIC     postalcityid STRING,
# MAGIC     postalpostalcode STRING,
# MAGIC     primarycontactpersonid STRING,
# MAGIC     runposition STRING,
# MAGIC     standarddiscountpercentage STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING,
# MAGIC     websiteurl STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.sales_customertransactions (
# MAGIC     amountexcludingtax STRING,
# MAGIC     business_month STRING,
# MAGIC     customerid STRING,
# MAGIC     customertransactionid STRING,
# MAGIC     finalizationdate STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     invoiceid STRING,
# MAGIC     isfinalized STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     outstandingbalance STRING,
# MAGIC     paymentmethodid STRING,
# MAGIC     taxamount STRING,
# MAGIC     transactionamount STRING,
# MAGIC     transactiondate STRING,
# MAGIC     transactiontypeid STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.sales_invoicelines (
# MAGIC     business_month STRING,
# MAGIC     description STRING,
# MAGIC     extendedprice STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     invoiceid STRING,
# MAGIC     invoicelineid STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     lineprofit STRING,
# MAGIC     packagetypeid STRING,
# MAGIC     quantity STRING,
# MAGIC     stockitemid STRING,
# MAGIC     taxamount STRING,
# MAGIC     taxrate STRING,
# MAGIC     unitprice STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.sales_invoices (
# MAGIC     accountspersonid STRING,
# MAGIC     billtocustomerid STRING,
# MAGIC     business_month STRING,
# MAGIC     comments STRING,
# MAGIC     confirmeddeliverytime STRING,
# MAGIC     confirmedreceivedby STRING,
# MAGIC     contactpersonid STRING,
# MAGIC     creditnotereason STRING,
# MAGIC     customerid STRING,
# MAGIC     customerpurchaseordernumber STRING,
# MAGIC     deliveryinstructions STRING,
# MAGIC     deliverymethodid STRING,
# MAGIC     deliveryrun STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     internalcomments STRING,
# MAGIC     invoicedate STRING,
# MAGIC     invoiceid STRING,
# MAGIC     iscreditnote STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     orderid STRING,
# MAGIC     packedbypersonid STRING,
# MAGIC     returneddeliverydata STRING,
# MAGIC     runposition STRING,
# MAGIC     salespersonpersonid STRING,
# MAGIC     totalchilleritems STRING,
# MAGIC     totaldryitems STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.sales_orderlines (
# MAGIC     business_month STRING,
# MAGIC     description STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     orderid STRING,
# MAGIC     orderlineid STRING,
# MAGIC     packagetypeid STRING,
# MAGIC     pickedquantity STRING,
# MAGIC     pickingcompletedwhen STRING,
# MAGIC     quantity STRING,
# MAGIC     stockitemid STRING,
# MAGIC     taxrate STRING,
# MAGIC     unitprice STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.sales_orders (
# MAGIC     backorderorderid STRING,
# MAGIC     business_month STRING,
# MAGIC     comments STRING,
# MAGIC     contactpersonid STRING,
# MAGIC     customerid STRING,
# MAGIC     customerpurchaseordernumber STRING,
# MAGIC     deliveryinstructions STRING,
# MAGIC     expecteddeliverydate STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     internalcomments STRING,
# MAGIC     isundersupplybackordered STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     orderdate STRING,
# MAGIC     orderid STRING,
# MAGIC     pickedbypersonid STRING,
# MAGIC     pickingcompletedwhen STRING,
# MAGIC     salespersonpersonid STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.sales_specialdeals (
# MAGIC     business_month STRING,
# MAGIC     buyinggroupid STRING,
# MAGIC     customercategoryid STRING,
# MAGIC     customerid STRING,
# MAGIC     dealdescription STRING,
# MAGIC     discountamount STRING,
# MAGIC     discountpercentage STRING,
# MAGIC     enddate STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     specialdealid STRING,
# MAGIC     startdate STRING,
# MAGIC     stockgroupid STRING,
# MAGIC     stockitemid STRING,
# MAGIC     unitprice STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.warehouse_coldroomtemperatures (
# MAGIC     business_month STRING,
# MAGIC     coldroomsensornumber STRING,
# MAGIC     coldroomtemperatureid STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     recordedwhen STRING,
# MAGIC     temperature STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.warehouse_colors (
# MAGIC     business_month STRING,
# MAGIC     colorid STRING,
# MAGIC     colorname STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.warehouse_packagetypes (
# MAGIC     business_month STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     packagetypeid STRING,
# MAGIC     packagetypename STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.warehouse_stockgroups (
# MAGIC     business_month STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     stockgroupid STRING,
# MAGIC     stockgroupname STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.warehouse_stockitemholdings (
# MAGIC     binlocation STRING,
# MAGIC     business_month STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lastcostprice STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     laststocktakequantity STRING,
# MAGIC     quantityonhand STRING,
# MAGIC     reorderlevel STRING,
# MAGIC     stockitemid STRING,
# MAGIC     targetstocklevel STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.warehouse_stockitems (
# MAGIC     barcode STRING,
# MAGIC     brand STRING,
# MAGIC     business_month STRING,
# MAGIC     colorid STRING,
# MAGIC     customfields STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     internalcomments STRING,
# MAGIC     ischillerstock STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     leadtimedays STRING,
# MAGIC     marketingcomments STRING,
# MAGIC     outerpackageid STRING,
# MAGIC     photo STRING,
# MAGIC     quantityperouter STRING,
# MAGIC     recommendedretailprice STRING,
# MAGIC     searchdetails STRING,
# MAGIC     size STRING,
# MAGIC     stockitemid STRING,
# MAGIC     stockitemname STRING,
# MAGIC     supplierid STRING,
# MAGIC     tags STRING,
# MAGIC     taxrate STRING,
# MAGIC     typicalweightperunit STRING,
# MAGIC     unitpackageid STRING,
# MAGIC     unitprice STRING,
# MAGIC     validfrom STRING,
# MAGIC     validto STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.warehouse_stockitemstockgroups (
# MAGIC     business_month STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     stockgroupid STRING,
# MAGIC     stockitemid STRING,
# MAGIC     stockitemstockgroupid STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.warehouse_stockitemtransactions (
# MAGIC     business_month STRING,
# MAGIC     customerid STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     invoiceid STRING,
# MAGIC     lasteditedby STRING,
# MAGIC     lasteditedwhen STRING,
# MAGIC     purchaseorderid STRING,
# MAGIC     quantity STRING,
# MAGIC     stockitemid STRING,
# MAGIC     stockitemtransactionid STRING,
# MAGIC     supplierid STRING,
# MAGIC     transactionoccurredwhen STRING,
# MAGIC     transactiontypeid STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wwi.bronze.warehouse_vehicletemperatures (
# MAGIC     business_month STRING,
# MAGIC     chillersensornumber STRING,
# MAGIC     compressedsensordata STRING,
# MAGIC     fullsensordata STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     iscompressed STRING,
# MAGIC     recordedwhen STRING,
# MAGIC     temperature STRING,
# MAGIC     vehicleregistration STRING,
# MAGIC     vehicletemperatureid STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

spark.sql("USE CATALOG wwi")
spark.sql("USE SCHEMA bronze")

tables = spark.sql("SHOW TABLES IN wwi.bronze").collect()

for table in tables:
    table_name = table.tableName
    spark.sql(f"DROP TABLE IF EXISTS wwi.bronze.{table_name}")
    print(f"Dropped: {table_name}")

print("All Bronze tables dropped successfully.")

# COMMAND ----------

