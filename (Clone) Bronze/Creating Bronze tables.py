# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create Bronze Table
# MAGIC CREATE OR REPLACE TABLE Bronze_Sales (
# MAGIC     InvoiceID STRING,
# MAGIC     Branch STRING,
# MAGIC     City STRING,
# MAGIC     CustomerType STRING,
# MAGIC     Gender STRING,
# MAGIC     ProductLine STRING,
# MAGIC     UnitPrice DOUBLE,
# MAGIC     Quantity INT,
# MAGIC     Tax DOUBLE,
# MAGIC     Total DOUBLE,
# MAGIC     Date DATE,
# MAGIC     Time TIMESTAMP,
# MAGIC     Payment STRING,
# MAGIC     COGS DOUBLE,
# MAGIC     GrossMarginPercentage DOUBLE,
# MAGIC     GrossIncome DOUBLE,
# MAGIC     Rating DOUBLE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from pyspark.sql.functions import col

# Define the file path in DBFS
file_path = "/FileStore/tables/supermarket_sales___Sheet1-2.csv"

# Load the CSV file into a DataFrame
raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

columns_to_rename = {
    "InvoiceID": "invoice_id",
    "Branch": "branch",
    "City": "city", 
    "CustomerType": "customer_type", 
    "Gender": "gender", 
    "ProductLine": "product_line", 
    "UnitPrice": "unit_price", 
    "Quantity": "quantity", 
    "Tax 5%": "tax", 
    "Total": "total", 
    "Date": "date", 
    "Time": "time", 
    "Payment": "payment", 
    "Rating": "rating"
    # Add all other columns here following the same pattern
}
# Define a function to convert column names to snake_case
def to_snake_case(col_name):
    return '_'.join([word.lower() for word in col_name.split()])

# Create a new DataFrame with renamed columns
transformed_df = raw_df
for original_name in raw_df.columns:
    new_name = to_snake_case(original_name)
    transformed_df = transformed_df.withColumnRenamed(original_name, new_name)
    clean_df = transformed_df.withColumnRenamed("tax_5%", "tax")

# Enable column mapping
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.catalog.clearCache()
# Save the DataFrame as a Bronze table with column mapping enabled
clean_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("Bronze_Salestable")

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if Bronze table is populated
# MAGIC SELECT * FROM Bronze_Salestable LIMIT ;
