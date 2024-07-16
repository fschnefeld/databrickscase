# Databricks notebook source
# MAGIC %md
# MAGIC ## A short example of what creating a DLT Gold table would look like

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE LIVE TABLE sales_order_in_la
# MAGIC COMMENT "Sales orders in LA."
# MAGIC TBLPROPERTIES ("quality" = "gold")
# MAGIC AS
# MAGIC SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr,
# MAGIC SUM(ordered_products_explode.price) as sales,
# MAGIC SUM(ordered_products_explode.qty) as qantity,
# MAGIC COUNT(ordered_products_explode.id) as product_count
# MAGIC FROM (
# MAGIC SELECT city, DATE(order_datetime) as order_date, customer_id, customer_name,
# MAGIC EXPLODE(ordered_products) as ordered_products_explode
# MAGIC FROM LIVE.sales_orders_cleaned
# MAGIC WHERE city = 'Los Angeles'
# MAGIC   )
# MAGIC GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr;
# MAGIC
# MAGIC CREATE LIVE TABLE sales_order_in_chicago
# MAGIC COMMENT "Sales orders in Chicago."
# MAGIC TBLPROPERTIES ("quality" = "gold")
# MAGIC AS
# MAGIC SELECT city, order_date, customer_id, customer_name,
# MAGIC ordered_products_explode.curr,
# MAGIC SUM(ordered_products_explode.price) as sales,
# MAGIC SUM(ordered_products_explode.qty) as qantity,
# MAGIC COUNT(ordered_products_explode.id) as product_count
# MAGIC FROM (
# MAGIC   SELECT city, DATE(order_datetime) as order_date, customer_id, customer_name,
# MAGIC EXPLODE(ordered_products) as ordered_products_explode
# MAGIC   FROM LIVE.sales_orders_cleaned
# MAGIC   WHERE city = 'Chicago'
# MAGIC   )
# MAGIC GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr;
