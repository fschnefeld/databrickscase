# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create Silver Table
# MAGIC CREATE OR REPLACE TABLE Silver_Sales (
# MAGIC     invoice_id STRING,
# MAGIC     branch STRING,
# MAGIC     city STRING,
# MAGIC     customer_type STRING,
# MAGIC     gender STRING,
# MAGIC     product_line STRING,
# MAGIC     unit_price DOUBLE,
# MAGIC     quantity INT,
# MAGIC     tax DOUBLE,
# MAGIC     total DOUBLE,
# MAGIC     date DATE,
# MAGIC     time TIMESTAMP,
# MAGIC     payment STRING,
# MAGIC     cogs DOUBLE,
# MAGIC     gross_margin_percentage DOUBLE,
# MAGIC     gross_income DOUBLE,
# MAGIC     rating DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transform and load data into Silver table
# MAGIC INSERT INTO Silver_Sales
# MAGIC SELECT 
# MAGIC     invoice_id,
# MAGIC     branch,
# MAGIC     city,
# MAGIC     customer_type,
# MAGIC     gender,
# MAGIC     product_line,
# MAGIC     unit_price,
# MAGIC     quantity,
# MAGIC     tax,
# MAGIC     total,
# MAGIC     date,
# MAGIC     time,
# MAGIC     payment,
# MAGIC     cogs,
# MAGIC     gross_margin_percentage,
# MAGIC     gross_income,
# MAGIC     rating
# MAGIC FROM Bronze_Salestable
# MAGIC WHERE date IS NOT NULL AND invoice_id IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if Silver table is populated
# MAGIC SELECT * FROM Silver_Sales LIMIT 10;
