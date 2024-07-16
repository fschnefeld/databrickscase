# Databricks notebook source
# MAGIC %md
# MAGIC ## Short view of what the creation of a Silver table looks like for non DLT's

# COMMAND ----------

# Load the bronze data
bronze_df = spark.table("intellishorefunctioning.default.bronze_salestable")

# Transformation logic for silver table
silver_df = bronze_df.dropDuplicates()

# Add a constraint for valid order_id
silver_df = silver_df.withColumn("order_id", expr("CASE WHEN order_id IS NULL THEN 'DROP ROW' ELSE order_id END"))

# Filter out the rows marked for dropping
silver_df = silver_df.filter(silver_df.order_id != "DROP ROW")

# Write the transformed data to the silver table
silver_df.write.format("delta").mode("overwrite").saveAsTable("intellishorefunctioning.default.silver_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## While it would look like this for a DLT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STREAMING LIVE TABLE sales_orders_cleaned(
# MAGIC   CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC PARTITIONED BY (order_date)
# MAGIC COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
# MAGIC TBLPROPERTIES ("quality" = "silver")
# MAGIC AS
# MAGIC SELECT f.customer_id, f.customer_name, f.number_of_line_items,
# MAGIC   TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime,
# MAGIC   DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date,
# MAGIC   f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
# MAGIC   FROM STREAM(LIVE.sales_orders_raw) f
# MAGIC   LEFT JOIN LIVE.customers c
# MAGIC       ON c.customer_id = f.customer_id
# MAGIC      AND c.customer_name = f.customer_name
