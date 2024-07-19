# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create Product Dimension Table
# MAGIC CREATE TABLE IF NOT EXISTS jewel_dim_product (
# MAGIC     product_id STRING PRIMARY KEY,
# MAGIC     product_type STRING,
# MAGIC     material STRING,
# MAGIC     stone STRING,
# MAGIC     color STRING
# MAGIC );
# MAGIC
# MAGIC -- Create Customer Dimension Table
# MAGIC CREATE TABLE IF NOT EXISTS jewel_dim_customer (
# MAGIC     customer_id STRING PRIMARY KEY NOT NULL,
# MAGIC     customer_name STRING,
# MAGIC     customer_segment STRING
# MAGIC );
# MAGIC
# MAGIC -- Create Store Dimension Table
# MAGIC CREATE TABLE IF NOT EXISTS jewel_dim_store (
# MAGIC     store_id STRING PRIMARY KEY,
# MAGIC     store_location STRING,
# MAGIC     store_type STRING
# MAGIC );
# MAGIC
# MAGIC -- Create Date Dimension Table
# MAGIC CREATE TABLE IF NOT EXISTS jewel_dim_date (
# MAGIC     created_at STRING PRIMARY KEY,
# MAGIC     date DATE,
# MAGIC     day INT,
# MAGIC     month INT,
# MAGIC     year INT,
# MAGIC     quarter INT
# MAGIC );
# MAGIC
# MAGIC -- Create Fact Table
# MAGIC CREATE TABLE IF NOT EXISTS fct_jewelry_sales (
# MAGIC     sale_id STRING PRIMARY KEY,
# MAGIC     timestamp TIMESTAMP,
# MAGIC     product_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     store_id STRING,
# MAGIC     quantity INT,
# MAGIC     price FLOAT,
# MAGIC     total_amount FLOAT,
# MAGIC     FOREIGN KEY (product_id) REFERENCES jewel_dim_product(product_id),
# MAGIC     FOREIGN KEY (customer_id) REFERENCES jewel_dim_customer(customer_id),
# MAGIC     FOREIGN KEY (store_id) REFERENCES jewel_dim_store(store_id)
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert data into jewel_dim_product
# MAGIC INSERT INTO jewel_dim_product (product_id, product_type, material, stone, color)
# MAGIC SELECT DISTINCT
# MAGIC     COALESCE(CAST(product_id AS STRING), 'Unknown') AS product_id,
# MAGIC     COALESCE(category_alias, 'Unknown') AS product_type,
# MAGIC     COALESCE(gold, 'Unknown') AS material,
# MAGIC     COALESCE(diamond, 'None') AS stone,
# MAGIC     COALESCE(red, 'Unknown') AS color
# MAGIC FROM hive_metastore.default.jewelry;
# MAGIC
# MAGIC -- Insert data into jewel_dim_customer
# MAGIC INSERT INTO jewel_dim_customer (customer_id, customer_name, customer_segment)
# MAGIC SELECT DISTINCT
# MAGIC     COALESCE(CAST(user_id AS STRING), 'Unknown') AS customer_id,
# MAGIC     'Unknown' AS customer_name,
# MAGIC     'Regular' AS customer_segment
# MAGIC FROM hive_metastore.default.jewelry
# MAGIC WHERE user_id IS NOT NULL; -- Add a filter to exclude NULL values
# MAGIC
# MAGIC -- Insert data into jewel_dim_store
# MAGIC INSERT INTO jewel_dim_store (store_id, store_location, store_type)
# MAGIC SELECT DISTINCT
# MAGIC     COALESCE(CAST(brand_id AS STRING), 'Unknown') AS store_id,
# MAGIC     'Unknown' AS store_location,
# MAGIC     'Physical' AS store_type
# MAGIC FROM hive_metastore.default.jewelry
# MAGIC WHERE brand_id IS NOT NULL; -- Add a filter to exclude NULL values
# MAGIC
# MAGIC -- Insert data into jewel_dim_date
# MAGIC INSERT INTO jewel_dim_date (created_at, date, day, month, year, quarter)
# MAGIC SELECT DISTINCT
# MAGIC     DATE_FORMAT(CAST(order_created_at AS TIMESTAMP), 'yyyyMMdd') AS created_at,
# MAGIC     CAST(order_created_at AS DATE) AS date,
# MAGIC     DAY(CAST(order_created_at AS DATE)) AS day,
# MAGIC     MONTH(CAST(order_created_at AS DATE)) AS month,
# MAGIC     YEAR(CAST(order_created_at AS DATE)) AS year,
# MAGIC     QUARTER(CAST(order_created_at AS DATE)) AS quarter
# MAGIC FROM hive_metastore.default.jewelry
# MAGIC WHERE order_created_at IS NOT NULL; -- Add a filter to exclude NULL values
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert data into fct_jewelry_sales
# MAGIC INSERT INTO fct_jewelry_sales (sale_id, timestamp, product_id, customer_id, store_id, quantity, price, total_amount)
# MAGIC SELECT
# MAGIC     CAST(order_id AS STRING) AS sale_id,
# MAGIC     order_created_at AS timestamp,
# MAGIC     CAST(product_id AS STRING) AS product_id,
# MAGIC     CAST(user_id AS STRING) AS customer_id,
# MAGIC     CAST(brand_id AS STRING) AS store_id,
# MAGIC     sku_quantity AS quantity,
# MAGIC     price_usd AS price,
# MAGIC     sku_quantity * price_usd AS total_amount
# MAGIC FROM hive_metastore.default.jewelry;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fct_jewelry_sales
