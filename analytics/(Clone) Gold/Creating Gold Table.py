# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create Gold Table
# MAGIC CREATE OR REPLACE TABLE Gold_Sales (
# MAGIC     invoice_id STRING PRIMARY KEY,
# MAGIC     date_key INT,
# MAGIC     time TIMESTAMP,
# MAGIC     branch_key INT,
# MAGIC     city_key INT,
# MAGIC     customer_Type_key INT,
# MAGIC     gender_key INT,
# MAGIC     product_line_key INT,
# MAGIC     unit_price DOUBLE,
# MAGIC     quantity INT,
# MAGIC     tax DOUBLE,
# MAGIC     total DOUBLE,
# MAGIC     payment_key INT,
# MAGIC     cogs DOUBLE,
# MAGIC     gross_margin_percentage DOUBLE,
# MAGIC     gross_income DOUBLE,
# MAGIC     rating DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Temporary views for dimension tables
# MAGIC CREATE OR REPLACE TEMP VIEW date_dim AS
# MAGIC SELECT date_key, date FROM DateDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW branch_dim AS
# MAGIC SELECT branch_key, branch, branch_id FROM BranchDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW city_dim AS
# MAGIC SELECT city_key, city FROM CityDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW customer_type_dim AS
# MAGIC SELECT customer_Type_key, customer_type FROM CustomerTypeDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW gender_dim AS
# MAGIC SELECT gender_key, gender FROM GenderDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW product_line_dim AS
# MAGIC SELECT product_line_key, product_line FROM ProductLineDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW payment_dim AS
# MAGIC SELECT payment_key, payment_method FROM PaymentDimension;
# MAGIC
# MAGIC -- Insert data into Gold Table
# MAGIC INSERT INTO Gold_Sales (invoice_id, date_key, time, branch_key, city_key, customer_Type_key, gender_key, product_line_key, unit_price, quantity, tax, total, payment_key, cogs, gross_margin_percentage, gross_income, rating)
# MAGIC SELECT 
# MAGIC     s.invoice_id,
# MAGIC     d.date_key,
# MAGIC     s.time,
# MAGIC     b.branch_key,
# MAGIC     c.city_key,
# MAGIC     ct.customer_Type_key,
# MAGIC     g.gender_key,
# MAGIC     p.product_line_key,
# MAGIC     s.unit_price,
# MAGIC     s.quantity,
# MAGIC     s.tax,
# MAGIC     s.total,
# MAGIC     pm.payment_key,
# MAGIC     s.cogs,
# MAGIC     s.gross_margin_percentage,
# MAGIC     s.gross_income,
# MAGIC     s.rating
# MAGIC FROM Silver_Sales s
# MAGIC JOIN date_dim d ON s.date = d.date
# MAGIC JOIN branch_dim b ON s.branch = b.branch -- Replace branch with BranchID or BranchKey or BranchName
# MAGIC JOIN city_dim c ON s.city = c.city
# MAGIC JOIN customer_type_dim ct ON s.customer_type = ct.customer_type
# MAGIC JOIN gender_dim g ON s.gender = g.gender
# MAGIC JOIN product_line_dim p ON s.product_line = p.product_line
# MAGIC JOIN payment_dim pm ON s.payment = pm.payment_method;
