# Databricks notebook source
# Load necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Define the file path in DBFS
file_path = "/FileStore/tables/supermarket_sales___Sheet1-2.csv"

# Create Spark session
spark = SparkSession.builder.appName("SupermarketSalesAnalysis").getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)

# Display the DataFrame
display(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Date Dimension Table
# MAGIC CREATE OR REPLACE TABLE DateDimension (
# MAGIC     date_key LONG GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
# MAGIC     date DATE,
# MAGIC     day INT,
# MAGIC     month INT,
# MAGIC     year INT,
# MAGIC     quarter INT,
# MAGIC     day_of_week STRING
# MAGIC );
# MAGIC
# MAGIC -- Branch Dimension Table
# MAGIC CREATE OR REPLACE TABLE BranchDimension (
# MAGIC     branch_key LONG GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
# MAGIC     branch_id STRING,
# MAGIC     branch STRING
# MAGIC );
# MAGIC
# MAGIC -- City Dimension Table
# MAGIC CREATE OR REPLACE TABLE CityDimension (
# MAGIC     city_key LONG GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     country STRING
# MAGIC );
# MAGIC
# MAGIC -- Customer Type Dimension Table
# MAGIC CREATE OR REPLACE TABLE CustomerTypeDimension (
# MAGIC     customer_type_key LONG GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
# MAGIC     customer_type STRING
# MAGIC );
# MAGIC
# MAGIC -- Gender Dimension Table
# MAGIC CREATE OR REPLACE TABLE GenderDimension (
# MAGIC     gender_key LONG GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
# MAGIC     gender STRING
# MAGIC );
# MAGIC
# MAGIC -- Product Line Dimension Table
# MAGIC CREATE OR REPLACE TABLE ProductLineDimension (
# MAGIC     product_line_key LONG GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
# MAGIC     product_line STRING
# MAGIC );
# MAGIC
# MAGIC -- Payment Dimension Table
# MAGIC CREATE OR REPLACE TABLE PaymentDimension (
# MAGIC     payment_key LONG GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
# MAGIC     payment_method STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sales Fact Table
# MAGIC CREATE OR REPLACE TABLE SalesFact (
# MAGIC     invoice_id STRING PRIMARY KEY,
# MAGIC     date_key LONG,
# MAGIC     time STRING,
# MAGIC     branch_key LONG,
# MAGIC     city_key LONG,
# MAGIC     customer_type_key LONG,
# MAGIC     gender_key LONG,
# MAGIC     product_line_key LONG,
# MAGIC     unit_price DECIMAL(10, 2),
# MAGIC     quantity INT,
# MAGIC     tax DECIMAL(10, 2),
# MAGIC     total DECIMAL(10, 2),
# MAGIC     payment_key LONG,
# MAGIC     cogs DECIMAL(10, 2),
# MAGIC     gross_margin_percentage DECIMAL(5, 2),
# MAGIC     gross_income DECIMAL(10, 2),
# MAGIC     rating DECIMAL(3, 1),
# MAGIC FOREIGN KEY (branch_key) REFERENCES BranchDimension(branch_key),
# MAGIC     FOREIGN KEY (city_key) REFERENCES CityDimension(city_key),
# MAGIC     FOREIGN KEY (customer_type_key) REFERENCES CustomerTypeDimension(customer_type_key),
# MAGIC     FOREIGN KEY (gender_key) REFERENCES GenderDimension(gender_key),
# MAGIC     FOREIGN KEY (product_line_key) REFERENCES ProductLineDimension(product_line_key),
# MAGIC     FOREIGN KEY (payment_key) REFERENCES PaymentDimension(payment_key),
# MAGIC     FOREIGN KEY (date_key) REFERENCES DateDimension(date_key));
# MAGIC

# COMMAND ----------

df.createOrReplaceTempView("raw_data")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Date Dimension Table
# MAGIC INSERT INTO DateDimension (date, day, month, year, quarter, day_of_week)
# MAGIC SELECT DISTINCT 
# MAGIC     to_date(Date, 'M/d/yyyy') AS date,
# MAGIC     day(to_date(Date, 'M/d/yyyy')) AS day,
# MAGIC     month(to_date(Date, 'M/d/yyyy')) AS month,
# MAGIC     year(to_date(Date, 'M/d/yyyy')) AS year,
# MAGIC     quarter(to_date(Date, 'M/d/yyyy')) AS quarter,
# MAGIC     date_format(to_date(Date, 'M/d/yyyy'), 'E') AS day_of_week
# MAGIC FROM raw_data;
# MAGIC
# MAGIC -- Branch Dimension Table
# MAGIC INSERT INTO BranchDimension (branch_id, branch)
# MAGIC SELECT DISTINCT Branch AS branch_id, Branch AS branch
# MAGIC FROM raw_data;
# MAGIC
# MAGIC -- City Dimension Table
# MAGIC INSERT INTO CityDimension (city)
# MAGIC SELECT DISTINCT City
# MAGIC FROM raw_data;
# MAGIC
# MAGIC -- Customer Type Dimension Table
# MAGIC INSERT INTO CustomerTypeDimension (customer_type)
# MAGIC SELECT DISTINCT `Customer type` AS customer_type
# MAGIC FROM raw_data;
# MAGIC
# MAGIC -- Gender Dimension Table
# MAGIC INSERT INTO GenderDimension (gender)
# MAGIC SELECT DISTINCT Gender
# MAGIC FROM raw_data;
# MAGIC
# MAGIC -- Product Line Dimension Table
# MAGIC INSERT INTO ProductLineDimension (product_line)
# MAGIC SELECT DISTINCT `Product line` AS product_line
# MAGIC FROM raw_data;
# MAGIC
# MAGIC -- Payment Dimension Table
# MAGIC INSERT INTO PaymentDimension (payment_method)
# MAGIC SELECT DISTINCT Payment AS payment_method
# MAGIC FROM raw_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Temporary views for dimension tables
# MAGIC CREATE OR REPLACE TEMP VIEW date_dim AS
# MAGIC SELECT DateKey, Date FROM DateDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW branch_dim AS
# MAGIC SELECT BranchKey, BranchID FROM BranchDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW city_dim AS
# MAGIC SELECT CityKey, City FROM CityDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW customer_type_dim AS
# MAGIC SELECT CustomerTypeKey, CustomerType FROM CustomerTypeDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW gender_dim AS
# MAGIC SELECT GenderKey, Gender FROM GenderDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW product_line_dim AS
# MAGIC SELECT ProductLineKey, ProductLine FROM ProductLineDimension;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW payment_dim AS
# MAGIC SELECT PaymentKey, PaymentMethod FROM PaymentDimension;
# MAGIC
# MAGIC -- Insert data into Sales Fact Table
# MAGIC INSERT INTO SalesFact (InvoiceID, `Date`, Time, BranchKey, CityKey, CustomerTypeKey, GenderKey, ProductLineKey, UnitPrice, Quantity, Tax, Total, PaymentKey, COGS, GrossMarginPercentage, GrossIncome, Rating)
# MAGIC SELECT 
# MAGIC     r.`Invoice ID` AS InvoiceID,
# MAGIC     to_date(r.Date, 'M/d/yyyy') AS `Date`,
# MAGIC     r.Time,
# MAGIC     b.BranchKey,
# MAGIC     c.CityKey,
# MAGIC     ct.CustomerTypeKey,
# MAGIC     g.GenderKey,
# MAGIC     p.ProductLineKey,
# MAGIC     r.`Unit price` AS UnitPrice,
# MAGIC     r.Quantity,
# MAGIC     r.`Tax 5%` AS Tax,
# MAGIC     r.Total,
# MAGIC     pm.PaymentKey,
# MAGIC     r.cogs AS COGS,
# MAGIC     r.`gross margin percentage` AS GrossMarginPercentage,
# MAGIC     r.`gross income` AS GrossIncome,
# MAGIC     r.Rating
# MAGIC FROM raw_data r
# MAGIC JOIN branch_dim b ON r.Branch = b.BranchID
# MAGIC JOIN city_dim c ON r.City = c.City
# MAGIC JOIN customer_type_dim ct ON r.`Customer type` = ct.CustomerType
# MAGIC JOIN gender_dim g ON r.Gender = g.Gender
# MAGIC JOIN product_line_dim p ON r.`Product line` = p.ProductLine
# MAGIC JOIN payment_dim pm ON r.Payment = pm.PaymentMethod;
